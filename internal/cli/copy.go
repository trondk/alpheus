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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"strings"
	"sync"
	"sync/atomic"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"alpheus-dev.org/alpheus/api/v1alpha1"
	"alpheus-dev.org/alpheus/internal/store"
)

// target is where results copy puts the results: a bucket and a prefix in
// it, empty or ending in /.
type target struct {
	bucket, prefix string
}

func parseTarget(v string) (target, error) {
	rest, ok := strings.CutPrefix(v, "s3://")
	bucket, prefix, _ := strings.Cut(rest, "/")
	if !ok || bucket == "" {
		return target{}, usagef("--to %q: want s3://<bucket>/<prefix>", v)
	}
	prefix = strings.TrimLeft(prefix, "/")
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	return target{bucket: bucket, prefix: prefix}, nil
}

func (t target) String() string { return "s3://" + t.bucket + "/" + t.prefix }

// copyCounts is how each chunk got to the target.
type copyCounts struct {
	server, streamed, there atomic.Int64
}

func resultsCopy(ctx context.Context, e *Env, args []string) error {
	var o opts
	flags := newFlags(e, "results copy", "<run> [<step>.<output> …] --to s3://<bucket>/<prefix> --to-secret <secret>", &o)
	var s3 s3Flags
	s3.add(flags)
	var sel selection
	sel.add(flags)
	to := flags.String("to", "", "where to copy to: s3://<bucket>/<prefix>")
	toSecret := flags.String("to-secret", "", "the Secret in -n with keys that can write there (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)")
	toEndpoint := flags.String("to-endpoint", "", "the target's S3 endpoint as seen from here (default: the run's storage)")
	toCAFile := flags.String("to-ca-file", "", "CA bundle for --to-endpoint (default: the run's storage's, when the endpoint is the same)")
	toRegion := flags.String("to-region", "", "the target bucket's region (default: the run's storage's, when the endpoint is the same)")
	force := flags.Bool("force", false, "copy to a prefix with other objects; only the run's own are overwritten")
	parallel := flags.Int("parallel", 4, "how many chunks to copy at a time")
	complete := flags.Bool("complete", false, "exit with an error when a result is incomplete")
	pos, err := parse(flags, args, &o)
	if err != nil {
		return err
	}
	if len(pos) < 1 {
		return usagef("results copy takes a run and optionally its results")
	}
	if *to == "" || *toSecret == "" {
		return usagef("results copy needs --to s3://<bucket>/<prefix> and --to-secret <secret>")
	}
	if *parallel < 1 {
		return usagef("--parallel %d: want 1 or more", *parallel)
	}
	tgt, err := parseTarget(*to)
	if err != nil {
		return err
	}
	sel.names = pos[1:]

	c, ns, err := e.connect(&o)
	if err != nil {
		return err
	}
	var r v1alpha1.StreamRun
	if err := c.Client.Get(ctx, client.ObjectKey{Namespace: ns, Name: pos[0]}, &r); err != nil {
		return notFound(err, ns, pos[0])
	}
	st, at, bucket, err := e.openResults(ctx, c.Client, &r, s3)
	if err != nil {
		return err
	}
	ix, err := index(ctx, st, &r, bucket, sel)
	if err != nil {
		return at.explain(err)
	}

	// The target, with its own keys; by default on the run's storage, as
	// seen from here.
	cfg := store.S3Config{Endpoint: *toEndpoint, Bucket: tgt.bucket, Region: *toRegion, CAFile: *toCAFile}
	if cfg.Endpoint == "" {
		cfg.Endpoint = at.Endpoint
		if cfg.Endpoint == "" {
			cfg.Endpoint = r.Spec.Storage.Endpoint
		}
		if cfg.CAFile == "" {
			cfg.CAFile, cfg.CAPEM = at.CAFile, at.CAPEM
		}
		if cfg.Region == "" {
			cfg.Region = r.Spec.Storage.Region
		}
	}
	var s corev1.Secret
	if err := c.Client.Get(ctx, client.ObjectKey{Namespace: r.Namespace, Name: *toSecret}, &s); err != nil {
		if isNotFound(err) {
			return fmt.Errorf("secret %s with the keys to the target not found in namespace %s", *toSecret, r.Namespace)
		}
		return apiError(err)
	}
	cfg.AccessKey, cfg.SecretKey = string(s.Data["AWS_ACCESS_KEY_ID"]), string(s.Data["AWS_SECRET_ACCESS_KEY"])
	dst, err := e.NewS3(cfg)
	if err != nil {
		return err
	}
	where := func(err error) error {
		if err == nil {
			return nil
		}
		return fmt.Errorf("target %s at %s: %w", tgt, cfg.Endpoint, err)
	}

	// The prefix must be empty, or the same run's (GO-148).
	old, err := readTargetIndex(ctx, dst, tgt)
	if err != nil {
		return where(err)
	}
	if old != nil && (old.Run != ix.Run || old.UID != ix.UID) {
		return hint(fmt.Errorf("%s has the results of another run, %s (UID %s)", tgt, old.Run, old.UID),
			"copy to another prefix; --force does not overwrite another run's objects")
	}
	if old == nil && !*force {
		for key, err := range dst.List(ctx, tgt.prefix) {
			if err != nil {
				return where(err)
			}
			return hint(fmt.Errorf("%s is not empty and has no %s: it has %s", tgt, IndexFile, key),
				"copy to another prefix, or to this one with --force, which only writes the run's own objects")
		}
	}

	// The index in the target names the copies.
	sources := make([]IndexChunk, len(ix.Chunks))
	copy(sources, ix.Chunks)
	for i := range ix.Chunks {
		ch := &ix.Chunks[i]
		ch.File = fileOf(*ch, false)
		ch.Key = tgt.prefix + ch.File
		ch.URL = "s3://" + tgt.bucket + "/" + ch.Key
	}
	ix.Bucket = tgt.bucket
	merged := mergeIndex(old, ix)
	merged.Complete = len(merged.incomplete()) == 0
	if err := writeTargetIndex(ctx, dst, tgt, merged); err != nil {
		return where(err)
	}

	var counts copyCounts
	if err := copyAll(ctx, st, dst, sources, ix.Chunks, *parallel, &counts); err != nil {
		return at.explain(err)
	}

	var total int64
	for _, ch := range ix.Chunks {
		total += ch.Bytes
	}
	if o.output != "" {
		if err := write(e.Stdout, o.output, merged, nil); err != nil {
			return err
		}
	} else {
		fmt.Fprintf(e.Stdout, "Copied %s (%s) to %s", plural(len(ix.Chunks), "chunk"), bytesString(total), tgt)
		var how []string
		if n := counts.server.Load(); n > 0 {
			how = append(how, fmt.Sprintf("%d by the storage", n))
		}
		if n := counts.streamed.Load(); n > 0 {
			how = append(how, fmt.Sprintf("%d through here", n))
		}
		if n := counts.there.Load(); n > 0 {
			how = append(how, fmt.Sprintf("%d were already there", n))
		}
		if len(how) > 0 {
			fmt.Fprintf(e.Stdout, ": %s", strings.Join(how, ", "))
		}
		fmt.Fprintln(e.Stdout)
	}
	warnIncomplete(e, ix)
	return incompleteErr(*complete, ix)
}

func readTargetIndex(ctx context.Context, dst store.Store, tgt target) (*Index, error) {
	data, err := store.ReadAll(ctx, dst, tgt.prefix+IndexFile)
	if errors.Is(err, store.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var ix Index
	if err := json.Unmarshal(data, &ix); err != nil {
		return nil, fmt.Errorf("%s%s: %w", tgt, IndexFile, err)
	}
	if ix.Version != IndexVersion {
		return nil, fmt.Errorf("%s%s has format version %d, which this alpheus does not know", tgt, IndexFile, ix.Version)
	}
	return &ix, nil
}

func writeTargetIndex(ctx context.Context, dst store.Store, tgt target, ix *Index) error {
	data, err := json.MarshalIndent(ix, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return dst.Put(ctx, tgt.prefix+IndexFile, bytes.NewReader(data), int64(len(data)))
}

// copyAll copies each chunk in from to its copy in to, parallel at a
// time, and stops at the first error.
func copyAll(ctx context.Context, src, dst store.Store, from, to []IndexChunk, parallel int, counts *copyCounts) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	work := make(chan int)
	var wg sync.WaitGroup
	for range parallel {
		wg.Go(func() {
			for i := range work {
				if err := copyChunk(ctx, src, dst, from[i], to[i], counts); err != nil {
					cancel(err)
				}
			}
		})
	}
	for i := range from {
		select {
		case work <- i:
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

// copyChunk copies one chunk (GO-155). A copy that is already there with
// the chunk's size is kept: objects are written whole or not at all, and
// the index says they are the run's. On the same endpoint the storage
// copies it; when the target's keys may not read the source, or on
// another endpoint, it is streamed through here and its SHA-256 checked
// before the upload completes, so no wrong copy is left.
func copyChunk(ctx context.Context, src, dst store.Store, from, to IndexChunk, counts *copyCounts) error {
	if n, err := store.Size(ctx, dst, to.Key); err == nil && n == to.Bytes {
		counts.there.Add(1)
		return nil
	} else if err != nil && !errors.Is(err, store.ErrNotFound) {
		return err
	}
	ok, err := store.ServerCopy(ctx, src, from.Key, dst, to.Key)
	if ok && err == nil {
		n, err := store.Size(ctx, dst, to.Key)
		if err != nil {
			return err
		}
		if n != from.Bytes {
			_ = dst.Delete(ctx, to.Key)
			return fmt.Errorf("the copy of %s has %d bytes, the manifest says %d", from.Key, n, from.Bytes)
		}
		counts.server.Add(1)
		return nil
	}
	if ok && !store.Refused(err) {
		return err
	}
	body, err := src.Get(ctx, from.Key)
	if err != nil {
		return err
	}
	defer body.Close()
	if err := dst.Put(ctx, to.Key, &checkedReader{r: body, c: from, h: sha256.New()}, from.Bytes); err != nil {
		return err
	}
	counts.streamed.Add(1)
	return nil
}

// checkedReader reads a chunk and checks it against its manifest entry.
// The read that would complete the chunk fails instead when size or
// SHA-256 do not match, so an upload of it is never completed.
type checkedReader struct {
	r io.Reader
	c IndexChunk
	h hash.Hash
	n int64
}

func (cr *checkedReader) Read(p []byte) (int, error) {
	n, err := cr.r.Read(p)
	cr.h.Write(p[:n])
	cr.n += int64(n)
	if cr.n > cr.c.Bytes {
		return 0, fmt.Errorf("chunk %s has more than the %d bytes the manifest says", cr.c.Key, cr.c.Bytes)
	}
	if cr.n == cr.c.Bytes || errors.Is(err, io.EOF) {
		if cerr := checkChunk(cr.c, cr.n, cr.h); cerr != nil {
			return 0, cerr
		}
	}
	return n, err
}
