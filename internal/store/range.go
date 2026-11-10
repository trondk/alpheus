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
	"errors"
	"fmt"
	"io"
)

// ErrChanged means the object is no longer the version asked for, so a
// download cannot go on where it stopped (GO-149).
var ErrChanged = errors.New("object changed since the download started")

// Ranger is a Store that can open an object from an offset, only while it
// is the version an ETag names, so a download that was stopped can go on
// where it stopped (GO-149).
type Ranger interface {
	// GetRange opens key from offset. With an etag, it fails with
	// ErrChanged when the object is another version. It returns the
	// object's ETag.
	GetRange(ctx context.Context, key string, offset int64, etag string) (io.ReadCloser, string, error)
}

// GetRange opens key from offset as Ranger does. A store that is not a
// Ranger can only open from the start and gives no ETag; from any other
// offset it returns ErrChanged, so the caller starts over.
func GetRange(ctx context.Context, s Store, key string, offset int64, etag string) (io.ReadCloser, string, error) {
	if r, ok := s.(Ranger); ok {
		return r.GetRange(ctx, key, offset, etag)
	}
	if offset > 0 || etag != "" {
		return nil, "", fmt.Errorf("get %s: %w", key, ErrChanged)
	}
	body, err := s.Get(ctx, key)
	return body, "", err
}
