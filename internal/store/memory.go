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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"iter"
	"slices"
	"strings"
	"sync"
)

// Memory is a Store in memory, for tests.
type Memory struct {
	mu      sync.Mutex
	objects map[string][]byte
}

// NewMemory returns an empty in-memory store.
func NewMemory() *Memory { return &Memory{objects: map[string][]byte{}} }

func (m *Memory) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("put %s: %w", key, err)
	}
	if size >= 0 && int64(len(data)) != size {
		return fmt.Errorf("put %s: read %d bytes, want %d", key, len(data), size)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("put %s: %w", key, err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objects[key] = data
	return nil
}

func (m *Memory) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("get %s: %w", key, err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.objects[key]
	if !ok {
		return nil, fmt.Errorf("get %s: %w", key, ErrNotFound)
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

// Size is the size of key.
func (m *Memory) Size(ctx context.Context, key string) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, fmt.Errorf("stat %s: %w", key, err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.objects[key]
	if !ok {
		return 0, fmt.Errorf("stat %s: %w", key, ErrNotFound)
	}
	return int64(len(data)), nil
}

// GetRange opens key from offset, as long as its ETag, the SHA-256 of its
// content, is etag.
func (m *Memory) GetRange(ctx context.Context, key string, offset int64, etag string) (io.ReadCloser, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", fmt.Errorf("get %s: %w", key, err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.objects[key]
	if !ok {
		return nil, "", fmt.Errorf("get %s: %w", key, ErrNotFound)
	}
	sum := sha256.Sum256(data)
	tag := hex.EncodeToString(sum[:])
	if etag != "" && etag != tag {
		return nil, "", fmt.Errorf("get %s: %w", key, ErrChanged)
	}
	if offset > int64(len(data)) {
		return nil, "", fmt.Errorf("get %s: offset %d beyond %d bytes", key, offset, len(data))
	}
	return io.NopCloser(bytes.NewReader(data[offset:])), tag, nil
}

func (m *Memory) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("delete %s: %w", key, err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, key)
	return nil
}

func (m *Memory) List(ctx context.Context, prefix string) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		m.mu.Lock()
		var keys []string
		for k := range m.objects {
			if strings.HasPrefix(k, prefix) {
				keys = append(keys, k)
			}
		}
		m.mu.Unlock()
		slices.Sort(keys)
		for _, k := range keys {
			if err := ctx.Err(); err != nil {
				yield("", fmt.Errorf("list %s: %w", prefix, err))
				return
			}
			if !yield(k, nil) {
				return
			}
		}
	}
}
