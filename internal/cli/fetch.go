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

package cli

import (
	"bufio"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"maps"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"alpheus-dev.org/alpheus/api/v1alpha1"
	"alpheus-dev.org/alpheus/internal/chunk"
	"alpheus-dev.org/alpheus/internal/controller"
	"alpheus-dev.org/alpheus/internal/store"
)

// IndexFile is the file results get writes next to the results it fetched
// (SPEC 9.1).
const IndexFile = "alpheus-results.json"

// IndexVersion is the format version of IndexFile and of results ls -o
// json (GO-162).
const IndexVersion = 1

// askAbove is the size above which results get asks before it fetches,
// unless --yes (GO-150).
const askAbove = 10 << 30

// Index is a run's committed results: what results ls prints and what
// results get writes to IndexFile, with the chunks it fetched.
type Index struct {
	Version   int    `json:"version"`
	Run       string `json:"run"`
	Namespace string `json:"namespace"`
	UID       string `json:"uid"`
	// Phase is the run's phase when the index was made.
	Phase string `json:"phase"`
	// Complete says the run succeeded and every replica of its results
	// ended its stream, so nothing more will come (GO-152).
	Complete bool `json:"complete"`
	// Bucket holds the results.
	Bucket   string         `json:"bucket"`
	Replicas []IndexReplica `json:"replicas"`
	Chunks   []IndexChunk   `json:"chunks"`
	// Errors are the error log's files results get --errors fetched,
	// relative to the folder.
	Errors []string `json:"errors,omitempty"`
}

// IndexReplica is one producer replica of a result.
type IndexReplica struct {
	// Result is <step>.<output>.
	Result  string `json:"result"`
	Replica int    `json:"replica"`
	// Complete says the replica ended its stream (EOS), so every chunk
	// it will ever have is listed (GO-152).
	Complete bool `json:"complete"`
	// Chunks is how many chunks the replica has committed, of which the
	// index may list only some (--replica, --chunks).
	Chunks int64 `json:"chunks"`
}

// IndexChunk is one committed chunk of a result.
type IndexChunk struct {
	Result  string `json:"result"`
	Replica int    `json:"replica"`
	N       int64  `json:"n"`
	// File is where results get puts it, relative to the folder.
	File string `json:"file,omitempty"`
	// Key is the object's key in Bucket; URL is s3://<bucket>/<key>.
	Key    string `json:"key"`
	URL    string `json:"url,omitempty"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

// selection is the part of a run's results a command works on (GO-151).
type selection struct {
	names    []string
	replica  int
	from, to int64
}

func (s *selection) add(fs *flag.FlagSet) {
	fs.IntVar(&s.replica, "replica", -1, "only this producer replica")
	fs.Func("chunks", "only these chunks of each replica: <n> or <from>-<to>, numbered from 1", func(v string) error {
		from, to, ok := strings.Cut(v, "-")
		var err1, err2 error
		s.from, err1 = strconv.ParseInt(from, 10, 64)
		s.to = s.from
		if ok {
			s.to, err2 = strconv.ParseInt(to, 10, 64)
		}
		if err1 != nil || err2 != nil || s.from < 1 || s.to < s.from {
			return fmt.Errorf("want <n> or <from>-<to>, from 1")
		}
		return nil
	})
}

func (s *selection) wants(replica int, n int64) bool {
	return (s.replica < 0 || replica == s.replica) && (s.from == 0 || (n >= s.from && n <= s.to))
}

// resultNames are a run's results, <step>.<output>, from status.results
// and what is in the bucket, in the order of the spec's steps.
func resultNames(ctx context.Context, st store.Store, r *v1alpha1.StreamRun) ([]string, error) {
	found := map[string]bool{}
	for _, res := range r.Status.Results {
		found[res.Step+"."+res.Output] = true
	}
	for _, s := range r.Spec.Steps {
		prefix := chunk.ResultsPrefix(r.Name, string(r.UID), s.Name)
		for key, err := range st.List(ctx, prefix) {
			if err != nil {
				return nil, err
			}
			// <output>/<replica>/manifest.json
			parts := strings.Split(strings.TrimPrefix(key, prefix), "/")
			if len(parts) == 3 && parts[2] == "manifest.json" {
				found[s.Name+"."+parts[0]] = true
			}
		}
	}
	var names []string
	for _, s := range r.Spec.Steps {
		var outs []string
		for name := range found {
			if step, _, _ := strings.Cut(name, "."); step == s.Name {
				outs = append(outs, name)
			}
		}
		slices.Sort(outs)
		names = append(names, outs...)
	}
	return names, nil
}

// index reads the manifests of the run's results, or the ones sel names,
// and lists the committed chunks sel wants. Only chunks in a manifest or
// its segments are listed: other objects under the prefix were never
// committed (GO-32).
func index(ctx context.Context, st store.Store, r *v1alpha1.StreamRun, bucket string, sel selection) (*Index, error) {
	all, err := resultNames(ctx, st, r)
	if err != nil {
		return nil, err
	}
	names := all
	if len(sel.names) > 0 {
		names = nil
		for _, n := range sel.names {
			name := n
			if !strings.Contains(n, ".") {
				name = n + "." + chunk.DefaultOutput
			}
			if !slices.Contains(all, name) {
				if len(all) == 0 {
					return nil, fmt.Errorf("run %s has no results yet", r.Name)
				}
				return nil, hint(fmt.Errorf("run %s has no result %q", r.Name, n), "its results are: "+strings.Join(all, ", "))
			}
			names = append(names, name)
		}
	}
	steps := map[string]*v1alpha1.Step{}
	for i := range r.Spec.Steps {
		steps[r.Spec.Steps[i].Name] = &r.Spec.Steps[i]
	}
	ix := &Index{Version: IndexVersion, Run: r.Name, Namespace: r.Namespace, UID: string(r.UID), Phase: string(r.Status.Phase),
		Bucket: bucket, Replicas: []IndexReplica{}, Chunks: []IndexChunk{}}
	for _, name := range names {
		step, output, _ := strings.Cut(name, ".")
		for i := range controller.Replicas(steps[step]) {
			if sel.replica >= 0 && i != sel.replica {
				continue
			}
			s := chunk.ResultStream(r.Name, string(r.UID), step, output, i)
			v, err := readStream(ctx, st, s, i)
			if err != nil {
				return nil, err
			}
			ix.Replicas = append(ix.Replicas, IndexReplica{Result: name, Replica: i, Complete: v.EOS, Chunks: int64(len(v.Chunks))})
			for _, c := range v.Chunks {
				if !sel.wants(i, c.N) {
					continue
				}
				key := s.ChunkKey(c.N, c.Ext)
				ix.Chunks = append(ix.Chunks, IndexChunk{Result: name, Replica: i, N: c.N, Key: key,
					URL: "s3://" + bucket + "/" + key, Bytes: c.Bytes, SHA256: c.SHA256})
			}
		}
	}
	ix.Complete = len(ix.incomplete()) == 0
	return ix, nil
}

// incomplete says why the results may not be all there, one line each:
// a run that did not succeed may have results it never committed a chunk
// of, and a replica without EOS has not committed all of its own (GO-152).
func (ix *Index) incomplete() []string {
	var out []string
	if ix.Phase != string(v1alpha1.RunSucceeded) {
		phase := ix.Phase
		if phase == "" {
			phase = "not started"
		}
		out = append(out, fmt.Sprintf("the run is %s, so a result may be missing or have only part of its chunks; %s committed",
			phase, plural(len(ix.Chunks), "chunk")))
	}
	for _, rp := range ix.Replicas {
		if !rp.Complete {
			out = append(out, fmt.Sprintf("%s replica %d has not ended its stream: %d chunks committed so far", rp.Result, rp.Replica, rp.Chunks))
		}
	}
	return out
}

// warnIncomplete says clearly that a result is not complete (GO-152).
func warnIncomplete(e *Env, ix *Index) {
	lines := ix.incomplete()
	if len(lines) == 0 {
		return
	}
	fmt.Fprintln(e.Stderr, "warning: the results are incomplete:")
	for _, l := range lines {
		fmt.Fprintf(e.Stderr, "  %s\n", l)
	}
}

// openRunResults reads the run and opens its results.
func openRunResults(ctx context.Context, e *Env, o *opts, name string, s3 s3Flags) (*v1alpha1.StreamRun, store.Store, storeAddr, string, error) {
	c, ns, err := e.connect(o)
	if err != nil {
		return nil, nil, storeAddr{}, "", err
	}
	var r v1alpha1.StreamRun
	if err := c.Client.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, &r); err != nil {
		return nil, nil, storeAddr{}, "", notFound(err, ns, name)
	}
	st, at, bucket, err := e.openResults(ctx, c.Client, &r, s3)
	return &r, st, at, bucket, err
}

func resultsLs(ctx context.Context, e *Env, args []string) error {
	var o opts
	fs := newFlags(e, "results ls", "<run> [<step>.<output> …]", &o)
	var s3 s3Flags
	s3.add(fs)
	var sel selection
	sel.add(fs)
	pos, err := parse(fs, args, &o)
	if err != nil {
		return err
	}
	if len(pos) < 1 {
		return usagef("results ls takes a run and optionally its results")
	}
	sel.names = pos[1:]
	r, st, at, bucket, err := openRunResults(ctx, e, &o, pos[0], s3)
	if err != nil {
		return err
	}
	ix, err := index(ctx, st, r, bucket, sel)
	if err != nil {
		return at.explain(err)
	}
	if o.output == "" {
		warnIncomplete(e, ix)
	}
	return write(e.Stdout, o.output, ix, func(w io.Writer) {
		fmt.Fprintln(w, "URL\tBYTES\tSHA256")
		for _, c := range ix.Chunks {
			fmt.Fprintf(w, "%s\t%d\t%s\n", c.URL, c.Bytes, c.SHA256)
		}
	})
}

func resultsCat(ctx context.Context, e *Env, args []string) error {
	var o opts
	fs := newFlags(e, "results cat", "<run> <step>.<output>", &o)
	var s3 s3Flags
	s3.add(fs)
	var sel selection
	sel.add(fs)
	pos, err := parse(fs, args, &o)
	if err != nil {
		return err
	}
	if len(pos) != 2 {
		return usagef("results cat takes a run and one of its results")
	}
	if o.output != "" {
		return usagef("results cat writes the chunks as they are; -o does not apply")
	}
	sel.names = pos[1:]
	r, st, at, bucket, err := openRunResults(ctx, e, &o, pos[0], s3)
	if err != nil {
		return err
	}
	ix, err := index(ctx, st, r, bucket, sel)
	if err != nil {
		return at.explain(err)
	}
	warnIncomplete(e, ix)
	return at.explain(cat(ctx, e.Stdout, st, ix))
}

// cat writes the chunks of the index to w in order, replica 0 first, and
// checks each against its size and SHA-256 (GO-151). A chunk that does not
// match is an error, after its bytes are written.
func cat(ctx context.Context, w io.Writer, st store.Store, ix *Index) error {
	chunks := slices.Clone(ix.Chunks)
	slices.SortStableFunc(chunks, func(a, b IndexChunk) int {
		return cmp.Or(cmp.Compare(a.Replica, b.Replica), cmp.Compare(a.N, b.N))
	})
	for _, c := range chunks {
		body, err := st.Get(ctx, c.Key)
		if err != nil {
			return err
		}
		h := sha256.New()
		n, err := io.Copy(io.MultiWriter(w, h), body)
		body.Close()
		if err != nil {
			return fmt.Errorf("%s: %w", c.Key, err)
		}
		if err := checkChunk(c, n, h); err != nil {
			return err
		}
	}
	return nil
}

// checkChunk compares what was read of a chunk with its manifest entry.
func checkChunk(c IndexChunk, n int64, h hash.Hash) error {
	if n != c.Bytes {
		return fmt.Errorf("chunk %s has %d bytes, the manifest says %d", c.Key, n, c.Bytes)
	}
	if sum := hex.EncodeToString(h.Sum(nil)); sum != c.SHA256 {
		return fmt.Errorf("chunk %s has SHA-256 %s, the manifest says %s", c.Key, sum, c.SHA256)
	}
	return nil
}

// fileOf is where results get puts a chunk: <result>/<replica>/part-<n>.<ext>,
// or <result>/<replica>-part-<n>.<ext> with --flat.
func fileOf(c IndexChunk, flat bool) string {
	base := path.Base(c.Key)
	if flat {
		return c.Result + "/" + strconv.Itoa(c.Replica) + "-" + base
	}
	return c.Result + "/" + strconv.Itoa(c.Replica) + "/" + base
}

func resultsGet(ctx context.Context, e *Env, args []string) error {
	var o opts
	flags := newFlags(e, "results get", "<run> [<step>.<output> …]", &o)
	var s3 s3Flags
	s3.add(flags)
	var sel selection
	sel.add(flags)
	dir := flags.String("d", "", "the folder to fetch to, made with its parents; - writes one result to stdout (default: ./<run>/)")
	flat := flags.Bool("flat", false, "put all chunks of a result in one folder, as <replica>-part-<n>.<ext>")
	force := flags.Bool("force", false, "fetch into a folder with other files; only the run's own files are overwritten")
	parallel := flags.Int("parallel", 4, "how many chunks to fetch at a time")
	yes := flags.Bool("yes", false, "fetch more than 10 GiB without asking")
	complete := flags.Bool("complete", false, "exit with an error when a result is incomplete")
	withErrors := flags.Bool("errors", false, "also fetch the error log, to _errors/<step>/<replica>/")
	pos, err := parse(flags, args, &o)
	if err != nil {
		return err
	}
	if len(pos) < 1 {
		return usagef("results get takes a run and optionally its results")
	}
	if *parallel < 1 {
		return usagef("--parallel %d: want 1 or more", *parallel)
	}
	sel.names = pos[1:]
	r, st, at, bucket, err := openRunResults(ctx, e, &o, pos[0], s3)
	if err != nil {
		return err
	}
	ix, err := index(ctx, st, r, bucket, sel)
	if err != nil {
		return at.explain(err)
	}
	if *dir == "-" {
		results := map[string]bool{}
		for _, rp := range ix.Replicas {
			results[rp.Result] = true
		}
		if len(results) != 1 {
			return usagef("-d - writes one result to stdout; name one of: %s", strings.Join(slices.Sorted(maps.Keys(results)), ", "))
		}
		warnIncomplete(e, ix)
		if err := cat(ctx, e.Stdout, st, ix); err != nil {
			return at.explain(err)
		}
		return incompleteErr(*complete, ix)
	}
	if *dir == "" {
		*dir = r.Name
	}
	for i := range ix.Chunks {
		ix.Chunks[i].File = fileOf(ix.Chunks[i], *flat)
	}

	// The folder must be empty, or the same run's (GO-148).
	old, err := readIndex(*dir)
	if err != nil {
		return err
	}
	if old != nil && (old.Run != ix.Run || old.UID != ix.UID) {
		return hint(fmt.Errorf("%s has the results of another run, %s (UID %s)", *dir, old.Run, old.UID),
			"fetch to another folder with -d; --force does not overwrite another run's files")
	}
	if old == nil && !*force {
		entries, err := os.ReadDir(*dir)
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		if len(entries) > 0 {
			return hint(fmt.Errorf("%s is not empty and has no %s", *dir, IndexFile),
				"fetch to another folder with -d, or use this one with --force, which only writes the run's own files")
		}
	}

	var errFiles []errorFile
	if *withErrors {
		if errFiles, err = errorLogFiles(ctx, st, r, ""); err != nil {
			return at.explain(err)
		}
	}

	// What is left to fetch, and is there room for it (GO-150)?
	var need int64
	todo := 0
	for _, c := range ix.Chunks {
		if left := remaining(filepath.Join(*dir, filepath.FromSlash(c.File)), c.Bytes); left > 0 {
			need += left
			todo++
		}
	}
	if err := os.MkdirAll(*dir, 0o755); err != nil {
		return err
	}
	if free, ok := freeSpace(*dir); ok && need > free {
		return hint(fmt.Errorf("the results need %s more and %s has %s free", bytesString(need), *dir, bytesString(free)),
			"fetch a part with --replica or --chunks, fetch to another disk with -d, or list the chunks as S3 paths with: alpheus results ls "+r.Name+" -n "+r.Namespace)
	}
	if need > askAbove && !*yes {
		fmt.Fprintf(e.Stderr, "Fetch %s of results to %s? [y/N] ", bytesString(need), *dir)
		answer, _ := bufio.NewReader(e.Stdin).ReadString('\n')
		if a := strings.ToLower(strings.TrimSpace(answer)); a != "y" && a != "yes" {
			fmt.Fprintln(e.Stderr, "Nothing fetched.")
			return nil
		}
	}

	// The index first, so a fetch that is stopped knows its own files.
	merged := mergeIndex(old, ix)
	merged.Complete = len(merged.incomplete()) == 0
	for _, f := range errFiles {
		merged.Errors = append(merged.Errors, f.file)
	}
	merged.Errors = slices.Compact(slices.Sorted(slices.Values(merged.Errors)))
	if err := writeIndex(*dir, merged); err != nil {
		return err
	}

	if err := fetchAll(ctx, st, *dir, ix.Chunks, *parallel); err != nil {
		return at.explain(err)
	}
	for _, f := range errFiles {
		if err := fetchWhole(ctx, st, f.key, filepath.Join(*dir, filepath.FromSlash(f.file))); err != nil {
			return at.explain(err)
		}
	}

	var total int64
	for _, c := range ix.Chunks {
		total += c.Bytes
	}
	if o.output != "" {
		if err := write(e.Stdout, o.output, merged, nil); err != nil {
			return err
		}
	} else {
		fmt.Fprintf(e.Stdout, "Fetched %s (%s) to %s/", plural(len(ix.Chunks), "chunk"), bytesString(total), strings.TrimSuffix(*dir, "/"))
		if done := len(ix.Chunks) - todo; done > 0 {
			fmt.Fprintf(e.Stdout, "; %d were already there", done)
		}
		fmt.Fprintln(e.Stdout)
		if len(errFiles) > 0 {
			fmt.Fprintf(e.Stdout, "The error log is in %s\n", filepath.Join(*dir, "_errors"))
		}
	}
	warnIncomplete(e, ix)
	return incompleteErr(*complete, ix)
}

// plural is n and the noun, with an s unless n is 1.
func plural(n int, noun string) string {
	if n == 1 {
		return "1 " + noun
	}
	return strconv.Itoa(n) + " " + noun + "s"
}

func incompleteErr(complete bool, ix *Index) error {
	if !complete || len(ix.incomplete()) == 0 {
		return nil
	}
	return errors.New("the results are incomplete, and --complete was given")
}

// remaining is how many bytes of a chunk are still to fetch to file.
func remaining(file string, size int64) int64 {
	if fi, err := os.Stat(file); err == nil && fi.Size() == size {
		return 0
	}
	if fi, err := os.Stat(file + partialSuffix); err == nil && fi.Size() <= size {
		return size - fi.Size()
	}
	return size
}

// readIndex reads the IndexFile in dir, or returns nil when there is none.
func readIndex(dir string) (*Index, error) {
	data, err := os.ReadFile(filepath.Join(dir, IndexFile))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var ix Index
	if err := json.Unmarshal(data, &ix); err != nil {
		return nil, fmt.Errorf("%s: %w", filepath.Join(dir, IndexFile), err)
	}
	if ix.Version != IndexVersion {
		return nil, fmt.Errorf("%s has format version %d, which this alpheus does not know", filepath.Join(dir, IndexFile), ix.Version)
	}
	return &ix, nil
}

// mergeIndex adds what a fetch has to what earlier fetches to the same
// folder had, so the index lists every file of the run in it.
func mergeIndex(old, ix *Index) *Index {
	m := *ix
	m.Replicas = slices.Clone(ix.Replicas)
	m.Chunks = slices.Clone(ix.Chunks)
	if old == nil {
		return &m
	}
	m.Errors = slices.Clone(old.Errors)
	for _, rp := range old.Replicas {
		if !slices.ContainsFunc(m.Replicas, func(n IndexReplica) bool { return n.Result == rp.Result && n.Replica == rp.Replica }) {
			m.Replicas = append(m.Replicas, rp)
		}
	}
	for _, c := range old.Chunks {
		if !slices.ContainsFunc(m.Chunks, func(n IndexChunk) bool { return n.File == c.File }) {
			m.Chunks = append(m.Chunks, c)
		}
	}
	slices.SortStableFunc(m.Replicas, func(a, b IndexReplica) int {
		return cmp.Or(cmp.Compare(a.Result, b.Result), cmp.Compare(a.Replica, b.Replica))
	})
	slices.SortStableFunc(m.Chunks, func(a, b IndexChunk) int {
		return cmp.Or(cmp.Compare(a.Result, b.Result), cmp.Compare(a.Replica, b.Replica), cmp.Compare(a.N, b.N))
	})
	return &m
}

func writeIndex(dir string, ix *Index) error {
	data, err := json.MarshalIndent(ix, "", "  ")
	if err != nil {
		return err
	}
	tmp := filepath.Join(dir, IndexFile+partialSuffix)
	if err := os.WriteFile(tmp, append(data, '\n'), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(dir, IndexFile))
}

// partialSuffix marks a file that is not whole yet (GO-149); etagSuffix
// keeps the version of the object it is a part of.
const (
	partialSuffix = ".partial"
	etagSuffix    = ".partial.etag"
)

// fetchAll fetches the chunks to dir, parallel at a time, and stops at the
// first error.
func fetchAll(ctx context.Context, st store.Store, dir string, chunks []IndexChunk, parallel int) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	work := make(chan IndexChunk)
	var wg sync.WaitGroup
	for range parallel {
		wg.Go(func() {
			for c := range work {
				if err := fetchChunk(ctx, st, filepath.Join(dir, filepath.FromSlash(c.File)), c); err != nil {
					cancel(err)
				}
			}
		})
	}
	for _, c := range chunks {
		select {
		case work <- c:
		case <-ctx.Done():
		}
		if ctx.Err() != nil {
			break
		}
	}
	close(work)
	wg.Wait()
	if err := context.Cause(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return ctx.Err()
}

// fetchChunk fetches one chunk to file (GO-149). It writes file.partial
// and renames it when size and SHA-256 match, so a file without .partial
// is always whole. A .partial from an earlier fetch is continued from
// where it stopped, if the object is still the version it came from. A
// chunk that does not match is an error, and nothing of it is left.
func fetchChunk(ctx context.Context, st store.Store, file string, c IndexChunk) error {
	if fi, err := os.Stat(file); err == nil && fi.Size() == c.Bytes {
		return nil
	}
	part, tagFile := file+partialSuffix, file+etagSuffix
	if err := os.MkdirAll(filepath.Dir(file), 0o755); err != nil {
		return err
	}
	var offset int64
	etag := ""
	if data, err := os.ReadFile(tagFile); err == nil {
		if fi, err := os.Stat(part); err == nil && fi.Size() <= c.Bytes {
			offset, etag = fi.Size(), strings.TrimSpace(string(data))
		}
	}
	h := sha256.New()
	if offset > 0 {
		f, err := os.Open(part)
		if err != nil {
			return err
		}
		_, err = io.CopyN(h, f, offset)
		f.Close()
		if err != nil {
			return err
		}
	}
	discard := func() {
		os.Remove(part)
		os.Remove(tagFile)
	}
	var body io.ReadCloser
	if offset < c.Bytes {
		var err error
		body, etag, err = store.GetRange(ctx, st, c.Key, offset, etag)
		if errors.Is(err, store.ErrChanged) {
			// Not the version the .partial came from: start over.
			offset = 0
			h.Reset()
			body, etag, err = store.GetRange(ctx, st, c.Key, 0, "")
		}
		if err != nil {
			return err
		}
		defer body.Close()
	}
	flags := os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	if offset > 0 {
		flags = os.O_WRONLY | os.O_APPEND
	}
	f, err := os.OpenFile(part, flags, 0o644)
	if err != nil {
		return err
	}
	if offset == 0 && etag != "" {
		if err := os.WriteFile(tagFile, []byte(etag+"\n"), 0o644); err != nil {
			f.Close()
			return err
		}
	}
	n := offset
	if body != nil {
		var m int64
		m, err = io.Copy(io.MultiWriter(f, h), body)
		n += m
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		// Stopped or broken off: the .partial stays, to go on from.
		return fmt.Errorf("%s: %w", c.Key, err)
	}
	if err := checkChunk(c, n, h); err != nil {
		discard()
		return err
	}
	if err := os.Rename(part, file); err != nil {
		return err
	}
	os.Remove(tagFile)
	return nil
}

// fetchWhole fetches a small object, such as an error log segment, to
// file through a .partial.
func fetchWhole(ctx context.Context, st store.Store, key, file string) error {
	data, err := store.ReadAll(ctx, st, key)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(file), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(file+partialSuffix, data, 0o644); err != nil {
		return err
	}
	return os.Rename(file+partialSuffix, file)
}
