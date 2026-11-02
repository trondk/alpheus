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

	"github.com/minio/minio-go/v7"
)

// Copy copies the object src to dst in st. When both are in S3 buckets on
// the same endpoint, the storage copies it without the bytes passing
// through here; otherwise it is read and written.
func Copy(ctx context.Context, st Store, src, dst string) error {
	from, to := resolve(st, src), resolve(st, dst)
	if fs, ok := from.(*S3); ok {
		if ts, ok := to.(*S3); ok && fs.client.EndpointURL().String() == ts.client.EndpointURL().String() {
			err := ts.retry.do(ctx, func() error {
				_, err := ts.client.ComposeObject(ctx,
					minio.CopyDestOptions{Bucket: ts.bucket, Object: dst},
					minio.CopySrcOptions{Bucket: fs.bucket, Object: src})
				return err
			})
			if err != nil {
				return fmt.Errorf("copy %s to %s: %w", src, dst, err)
			}
			return nil
		}
	}
	r, err := st.Get(ctx, src)
	if err != nil {
		return err
	}
	defer r.Close()
	return st.Put(ctx, dst, r, -1)
}

// resolve is the store that holds key, looking through WithResults.
func resolve(st Store, key string) Store {
	if r, ok := st.(*routed); ok {
		return r.of(key)
	}
	return st
}
