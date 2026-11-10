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

package agent

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"iter"
	"os"
	"strings"
	"testing"

	"alpheus-dev.org/alpheus/internal/store"
)

// s3Store connects to the bucket in ALPHEUS_TEST_S3_*, or skips the test.
// The store keeps the test's objects under a prefix of their own and
// deletes them afterwards.
func s3Store(t *testing.T) store.Store {
	t.Helper()
	endpoint := os.Getenv("ALPHEUS_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("ALPHEUS_TEST_S3_ENDPOINT not set")
	}
	s, err := store.NewS3(store.S3Config{
		Endpoint:  endpoint,
		Bucket:    os.Getenv("ALPHEUS_TEST_S3_BUCKET"),
		Region:    os.Getenv("ALPHEUS_TEST_S3_REGION"),
		AccessKey: os.Getenv("ALPHEUS_TEST_S3_ACCESS_KEY"),
		SecretKey: os.Getenv("ALPHEUS_TEST_S3_SECRET_KEY"),
		CAFile:    os.Getenv("ALPHEUS_TEST_S3_CA_FILE"),
	})
	if err != nil {
		t.Fatal(err)
	}
	id := make([]byte, 6)
	rand.Read(id)
	p := prefixed{s: s, prefix: "agent-test/" + hex.EncodeToString(id) + "/"}
	t.Cleanup(func() {
		if err := store.DeletePrefix(context.Background(), s, p.prefix); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	})
	return p
}

// prefixed puts every key under a prefix.
type prefixed struct {
	s      store.Store
	prefix string
}

func (p prefixed) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	return p.s.Put(ctx, p.prefix+key, r, size)
}

func (p prefixed) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	return p.s.Get(ctx, p.prefix+key)
}

func (p prefixed) Delete(ctx context.Context, key string) error {
	return p.s.Delete(ctx, p.prefix+key)
}

func (p prefixed) List(ctx context.Context, prefix string) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		for key, err := range p.s.List(ctx, p.prefix+prefix) {
			if !yield(strings.TrimPrefix(key, p.prefix), err) {
				return
			}
		}
	}
}

// TestS3Pipeline runs testcase03's shape against a real bucket, with
// signals through the hub and a restart of the stateful step, for example
// against Garage in the Vagrant cluster:
//
//	make test-agent-s3
func TestS3Pipeline(t *testing.T) {
	st := s3Store(t)
	if err := run(t, st, hub(t), pipeline(30, 10)...); err != nil {
		t.Fatal(err)
	}
	checkPipeline(t, st, 30, 10)
}

// TestS3Restart runs the pipeline against a real bucket, crashes the sum
// after five chunks, and runs it again as a new attempt: the result must be
// as if nothing happened. It runs with make test-s3, and is skipped without
// ALPHEUS_TEST_S3_ENDPOINT.
func TestS3Restart(t *testing.T) {
	st := s3Store(t)
	rs := pipeline(12, 3)
	crashed := append([]replica(nil), rs...)
	crashed[4].h = crashAfter(5, rs[4].h)
	if err := run(t, st, nil, crashed...); err == nil {
		t.Fatal("first attempt did not crash")
	}
	for j := range rs {
		rs[j].cfg.Attempt = 2
	}
	if err := run(t, st, hub(t), rs...); err != nil {
		t.Fatal(err)
	}
	checkPipeline(t, st, 12, 3)
}
