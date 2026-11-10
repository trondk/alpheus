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
	"context"
	"fmt"
	"io"

	"github.com/minio/minio-go/v7"
)

// Copy copies the object src to dst in st. When both are in S3 buckets on
// the same endpoint, the storage copies it without the bytes passing
// through here; otherwise it is read and written.
func Copy(ctx context.Context, st Store, src, dst string) error {
	if ok, err := ServerCopy(ctx, st, src, st, dst); ok || err != nil {
		return err
	}
	r, err := st.Get(ctx, src)
	if err != nil {
		return err
	}
	defer r.Close()
	return st.Put(ctx, dst, r, -1)
}

// ServerCopy has the storage copy src in from to dst in to, without the
// bytes passing through here. It does so only when both are S3 buckets on
// the same endpoint, and reports false without trying otherwise. The
// copy is made with to's keys, which must be able to read src.
func ServerCopy(ctx context.Context, from Store, src string, to Store, dst string) (bool, error) {
	fs, ok := resolve(from, src).(*S3)
	if !ok {
		return false, nil
	}
	ts, ok := resolve(to, dst).(*S3)
	if !ok || fs.client.EndpointURL().String() != ts.client.EndpointURL().String() {
		return false, nil
	}
	err := ts.retry.do(ctx, func() error {
		_, err := ts.client.ComposeObject(ctx,
			minio.CopyDestOptions{Bucket: ts.bucket, Object: dst},
			minio.CopySrcOptions{Bucket: fs.bucket, Object: src})
		return err
	})
	if err != nil {
		return true, fmt.Errorf("copy %s to %s: %w", src, dst, err)
	}
	return true, nil
}

// Sizer is a Store that can tell an object's size without reading it.
type Sizer interface {
	// Size is the size of key, or ErrNotFound.
	Size(ctx context.Context, key string) (int64, error)
}

// Size is the size of key: from a Sizer, or else by reading the object.
func Size(ctx context.Context, st Store, key string) (int64, error) {
	if s, ok := st.(Sizer); ok {
		return s.Size(ctx, key)
	}
	body, err := st.Get(ctx, key)
	if err != nil {
		return 0, err
	}
	defer body.Close()
	return io.Copy(io.Discard, body)
}

// resolve is the store that holds key, looking through WithResults.
func resolve(st Store, key string) Store {
	if r, ok := st.(*routed); ok {
		return r.of(key)
	}
	return st
}
