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

package store

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"slices"
	"strings"
	"testing"

	"alpheus-dev.org/alpheus/internal/chunk"
)

// TestMemory runs the store checks against the in-memory store the other
// tests use: a missing object, put, overwrite and get, unknown size, a chunk
// with its checksum, list and delete a prefix, size, and a ranged read of
// the same version.
func TestMemory(t *testing.T) {
	testStore(t, NewMemory(), "test/")
}

// TestS3 runs the same checks against a real bucket when ALPHEUS_TEST_S3_*
// is set, for example against Garage in the Vagrant cluster:
//
//	make test-s3
func TestS3(t *testing.T) {
	endpoint := os.Getenv("ALPHEUS_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("ALPHEUS_TEST_S3_ENDPOINT not set")
	}
	s, err := NewS3(S3Config{
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
	// A prefix of its own, so test runs never meet each other's objects.
	id := make([]byte, 6)
	rand.Read(id)
	prefix := "store-test/" + hex.EncodeToString(id) + "/"
	t.Cleanup(func() {
		if err := DeletePrefix(context.Background(), s, prefix); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	})
	testStore(t, s, prefix)
}

// TestNewS3RejectsBadEndpoints checks that an endpoint without http:// or
// https://, with another scheme or with a path is refused.
func TestNewS3RejectsBadEndpoints(t *testing.T) {
	for _, endpoint := range []string{"s3.example:3900", "ftp://s3.example", "https://s3.example/path", ""} {
		if _, err := NewS3(S3Config{Endpoint: endpoint, Bucket: "b"}); err == nil {
			t.Errorf("NewS3(%q) accepted", endpoint)
		}
	}
}

// testStore checks the behaviour the agent relies on, under prefix.
func testStore(t *testing.T, s Store, prefix string) {
	ctx := context.Background()
	key := func(k string) string { return prefix + k }

	t.Run("missing object", func(t *testing.T) {
		if _, err := s.Get(ctx, key("missing")); !errors.Is(err, ErrNotFound) {
			t.Errorf("Get: %v, want ErrNotFound", err)
		}
		if err := s.Delete(ctx, key("missing")); err != nil {
			t.Errorf("Delete: %v", err)
		}
	})

	t.Run("put, overwrite, get", func(t *testing.T) {
		put(t, s, key("manifest.json"), "first")
		put(t, s, key("manifest.json"), "second")
		if got := get(t, s, key("manifest.json")); got != "second" {
			t.Errorf("after overwrite: %q", got)
		}
	})

	t.Run("unknown size", func(t *testing.T) {
		content := strings.Repeat("x", 1<<20)
		if err := s.Put(ctx, key("unknown-size"), strings.NewReader(content), -1); err != nil {
			t.Fatal(err)
		}
		if got := get(t, s, key("unknown-size")); got != content {
			t.Errorf("got %d bytes, want %d", len(got), len(content))
		}
	})

	t.Run("chunk with checksum", func(t *testing.T) {
		// What the agent does: hash while writing, verify while reading.
		content := bytes.Repeat([]byte("order_id,country\n"), 100000)
		var buf bytes.Buffer
		hw := chunk.NewHashingWriter(&buf)
		hw.Write(content)
		c := chunk.Chunk{N: 1, Ext: "csv", Bytes: hw.Size(), SHA256: hw.Sum()}
		if err := s.Put(ctx, key("part-000001.csv"), &buf, c.Bytes); err != nil {
			t.Fatal(err)
		}
		r, err := s.Get(ctx, key("part-000001.csv"))
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		got, err := io.ReadAll(chunk.NewVerifyingReader(r, c))
		if err != nil || !bytes.Equal(got, content) {
			t.Errorf("read back: %d bytes, %v", len(got), err)
		}
	})

	t.Run("list and delete prefix", func(t *testing.T) {
		for _, k := range []string{"run/a/2", "run/a/1", "run/b/1", "runner/x"} {
			put(t, s, key(k), k)
		}
		var keys []string
		for k, err := range s.List(ctx, key("run/")) {
			if err != nil {
				t.Fatal(err)
			}
			keys = append(keys, strings.TrimPrefix(k, prefix))
		}
		if want := []string{"run/a/1", "run/a/2", "run/b/1"}; !slices.Equal(keys, want) {
			t.Errorf("List = %v, want %v", keys, want)
		}

		// Leaving the loop early must not hang or leak.
		for range s.List(ctx, key("run/")) {
			break
		}

		if err := DeletePrefix(ctx, s, key("run/")); err != nil {
			t.Fatal(err)
		}
		for k, err := range s.List(ctx, key("run/")) {
			t.Errorf("left after DeletePrefix: %s %v", k, err)
		}
		if got := get(t, s, key("runner/x")); got != "runner/x" {
			t.Errorf("DeletePrefix removed a neighbour: %q", got)
		}
	})

	// A download that stopped goes on from where it stopped, only while
	// the object is the same version (GO-149).
	t.Run("size", func(t *testing.T) {
		k := key("sized.bin")
		put(t, s, k, "0123456789")
		if n, err := Size(ctx, s, k); err != nil || n != 10 {
			t.Fatalf("size %d, %v; want 10", n, err)
		}
		if _, err := Size(ctx, s, key("no-such-object")); !errors.Is(err, ErrNotFound) {
			t.Fatalf("size of a missing object: %v, want ErrNotFound", err)
		}
	})

	t.Run("range of the same version", func(t *testing.T) {
		k := key("part-000001.bin")
		put(t, s, k, "0123456789")
		body, etag, err := GetRange(ctx, s, k, 0, "")
		if err != nil {
			t.Fatal(err)
		}
		body.Close()
		if etag == "" {
			t.Fatal("no ETag")
		}
		body, _, err = GetRange(ctx, s, k, 6, etag)
		if err != nil {
			t.Fatal(err)
		}
		rest, err := io.ReadAll(body)
		body.Close()
		if err != nil || string(rest) != "6789" {
			t.Errorf("from 6: %q %v, want 6789", rest, err)
		}
		put(t, s, k, "another version")
		if _, _, err := GetRange(ctx, s, k, 6, etag); !errors.Is(err, ErrChanged) {
			t.Errorf("after an overwrite: %v, want ErrChanged", err)
		}
	})
}

func put(t *testing.T, s Store, key, content string) {
	t.Helper()
	if err := s.Put(context.Background(), key, strings.NewReader(content), int64(len(content))); err != nil {
		t.Fatal(err)
	}
}

func get(t *testing.T, s Store, key string) string {
	t.Helper()
	data, err := ReadAll(context.Background(), s, key)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
