/*
Copyright The Alpheus Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package replay is what a failed attempt leaves so it can be run again on
// its own (GO-141): the failure record, finding it, fetching its input
// chunks checked against their SHA-256, and the environment that runs the
// component on them in local mode.
package replay

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"alpheus-dev.org/alpheus/api/v1alpha1"
	"alpheus-dev.org/alpheus/internal/chunk"
	"alpheus-dev.org/alpheus/internal/store"
)

// MaxInputs caps how many input chunks a failure keeps.
const MaxInputs = 16

// MaxSkipped caps how many input chunks are kept of a replica that gave up
// with failurePolicy continue (GO-97).
const MaxSkipped = 10000

// Failure is what a failed attempt leaves so it can be replayed on its own
// (GO-141): results/<run>/<uid>/_failures/<step>/<replica>/attempt-<n>/
// failure.json, with copies of the input chunks the attempt was given but
// had not finished, in input/, as files named like the agent names them.
// Images are pinned by digest and chunks are checked by their SHA-256, so
// a replay runs the same code on the same bytes.
type Failure struct {
	Version   int             `json:"version"`
	Run       string          `json:"run"`
	Namespace string          `json:"namespace"`
	UID       string          `json:"uid"`
	Step      string          `json:"step"`
	Replica   int             `json:"replica"`
	Replicas  int             `json:"replicas"`
	Attempt   int             `json:"attempt"`
	Reason    v1alpha1.Reason `json:"reason"`
	Message   string          `json:"message"`
	Time      time.Time       `json:"time"`
	Image     string          `json:"image"`
	Settings  json.RawMessage `json:"settings,omitempty"`
	// Source is true for a step without inputs: a replay runs it with
	// none.
	Source bool `json:"source,omitempty"`
	// Skipped is true for the record of every input chunk a replica never
	// processed after it gave up with failurePolicy continue, kept in
	// <replica>/skipped/ when the run finished, instead of one attempt's.
	// NotKept counts those that could not be kept, such as those beyond
	// MaxSkipped.
	Skipped bool    `json:"skipped,omitempty"`
	NotKept int64   `json:"notKept,omitempty"`
	Inputs  []Input `json:"inputs"`
	// Logs is where the attempt's logs are kept (GO-140).
	Logs string `json:"logs"`
}

// Input is one input chunk of a failed attempt.
type Input struct {
	Port            string `json:"port"`
	Edge            string `json:"edge"`
	ProducerReplica int    `json:"producerReplica"`
	N               int64  `json:"n"`
	Bytes           int64  `json:"bytes"`
	SHA256          string `json:"sha256"`
	// File is the copy's name in input/.
	File string `json:"file"`
	// Side marks a chunk of a side input, which the replay gets whole
	// (GO-167).
	Side bool `json:"side,omitempty"`
}

// LocalEnv is the environment that runs the component in local mode on
// the failure's inputs, at debug level (GO-141). The inputs are only this
// replica's, so it runs as replica 0 of 1.
func LocalEnv(f *Failure, in, out string) []string {
	settings := string(f.Settings)
	if settings == "" {
		settings = "{}"
	}
	env := []string{"ALPHEUS_MODE=local", "ALPHEUS_LOCAL_OUTPUT=" + out, "ALPHEUS_LOG_LEVEL=debug",
		"ALPHEUS_REPLICA=0", "ALPHEUS_REPLICAS=1", "ALPHEUS_SETTINGS=" + settings}
	var ports []string
	for _, p := range Ports(f) {
		ports = append(ports, p+"="+path.Join(in, p))
	}
	switch {
	case len(ports) == 1 && strings.HasPrefix(ports[0], "default="):
		env = append(env, "ALPHEUS_LOCAL_INPUT="+path.Join(in, "default"))
	case len(ports) > 0:
		env = append(env, "ALPHEUS_LOCAL_INPUTS="+strings.Join(ports, ","))
	}
	return env
}

func Ports(f *Failure) []string {
	var ports []string
	for _, in := range f.Inputs {
		if !slices.Contains(ports, in.Port) {
			ports = append(ports, in.Port)
		}
	}
	sort.Strings(ports)
	return ports
}

// Find is the prefix of a kept failure: the given attempt, or the
// last one.
func Find(ctx context.Context, st store.Store, run, uid, step string, replica, attempt int) (string, error) {
	if attempt > 0 {
		return chunk.FailurePrefix(run, uid, step, replica, attempt), nil
	}
	base := chunk.FailurePrefix(run, uid, step, replica, 1)
	base = base[:strings.LastIndex(strings.TrimSuffix(base, "/"), "/")+1]
	last := 0
	for key, err := range st.List(ctx, base) {
		if err != nil {
			return "", err
		}
		rest, ok := strings.CutPrefix(key, base+"attempt-")
		if !ok {
			continue
		}
		n, _, _ := strings.Cut(rest, "/")
		if a, err := strconv.Atoi(n); err == nil && a > last {
			last = a
		}
	}
	if last == 0 {
		return "", fmt.Errorf("step %s replica %d of run %s has no kept failure", step, replica, run)
	}
	return chunk.FailurePrefix(run, uid, step, replica, last), nil
}

// Fetch downloads the failure's input chunks into dir/in/<port>/,
// checked against their size and SHA-256, and makes dir/out/ for the
// output, writable by the component's user.
func Fetch(ctx context.Context, st store.Store, prefix string, f *Failure, dir string) error {
	out := filepath.Join(dir, "out")
	if err := os.RemoveAll(out); err != nil {
		return err
	}
	if err := os.MkdirAll(out, 0o777); err != nil {
		return err
	}
	if err := os.Chmod(out, 0o777); err != nil { // the image's user is not this one
		return err
	}
	for _, in := range f.Inputs {
		pdir := filepath.Join(dir, "in", in.Port)
		if err := os.MkdirAll(pdir, 0o755); err != nil {
			return err
		}
		if err := fetchChunk(ctx, st, prefix+"input/"+in.File, filepath.Join(pdir, in.File), in); err != nil {
			return err
		}
	}
	return nil
}

func fetchChunk(ctx context.Context, st store.Store, key, file string, in Input) error {
	r, err := st.Get(ctx, key)
	if err != nil {
		return err
	}
	defer r.Close()
	w, err := os.Create(file)
	if err != nil {
		return err
	}
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(w, h), r)
	if cerr := w.Close(); err == nil {
		err = cerr
	}
	if err == nil && (n != in.Bytes || hex.EncodeToString(h.Sum(nil)) != in.SHA256) {
		err = errors.New("its size or SHA-256 differs from the manifest's")
	}
	if err != nil {
		os.Remove(file)
		return fmt.Errorf("input %s: %w", in.File, err)
	}
	return nil
}

// Read reads a failure record.
func Read(ctx context.Context, st store.Store, prefix string) (*Failure, []byte, error) {
	data, err := store.ReadAll(ctx, st, prefix+"failure.json")
	if err != nil {
		return nil, nil, err
	}
	f, err := Parse(data)
	if err != nil {
		return nil, nil, fmt.Errorf("%sfailure.json: %w", prefix, err)
	}
	return f, data, nil
}

var (
	portPattern = regexp.MustCompile(`^[a-z]([a-z0-9-]*[a-z0-9])?$`)
	// filePattern is how the controller names an input copy:
	// <edge>-<producer replica>-<n>.<ext>.
	filePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*-[0-9]+-[0-9]{6,}\.[a-z0-9]{1,16}$`)
	hexPattern  = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

// Parse decodes a failure record and checks its inputs. The record comes
// from the bucket and its ports and file names become paths on the
// machine that replays it, so each must be one plain name, never a path.
func Parse(data []byte) (*Failure, error) {
	var f Failure
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, err
	}
	limit := MaxInputs
	if f.Skipped {
		limit = MaxSkipped
	}
	if len(f.Inputs) > limit {
		return nil, fmt.Errorf("%d inputs, at most %d", len(f.Inputs), limit)
	}
	if f.NotKept < 0 {
		return nil, fmt.Errorf("notKept %d", f.NotKept)
	}
	for i, in := range f.Inputs {
		switch {
		case len(in.Port) > 20 || !portPattern.MatchString(in.Port):
			return nil, fmt.Errorf("input %d: port %q is not a port name", i, in.Port)
		case len(in.File) > 255 || !filePattern.MatchString(in.File):
			return nil, fmt.Errorf("input %d: %q is not an input file name", i, in.File)
		case in.N < 1 || in.Bytes < 0 || !hexPattern.MatchString(in.SHA256):
			return nil, fmt.Errorf("input %d: chunk %d with %d bytes and SHA-256 %q", i, in.N, in.Bytes, in.SHA256)
		}
	}
	return &f, nil
}
