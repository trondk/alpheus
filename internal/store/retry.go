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
	"net"
	"net/http"
	"syscall"
	"time"

	"github.com/minio/minio-go/v7"
)

// ErrUnreachable means the storage did not answer, or answered with a
// temporary error, for longer than S3Config.Timeout (GO-136).
var ErrUnreachable = errors.New("storage unreachable")

// Backoff between attempts while the storage is unreachable.
var (
	minWait = 500 * time.Millisecond
	maxWait = 15 * time.Second
)

// transient reports whether err may go away by itself: the storage is down,
// restarting, overloaded or cut off. Errors that say the request is wrong,
// such as a missing object, a refused key or a full bucket, are not.
func transient(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var r minio.ErrorResponse
	if errors.As(err, &r) && r.StatusCode != 0 {
		return r.StatusCode >= 500 || r.StatusCode == http.StatusTooManyRequests || r.StatusCode == http.StatusRequestTimeout
	}
	var ne net.Error
	return errors.As(err, &ne) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EPIPE)
}

// Refused reports whether the storage turned the request away because of
// the keys or the bucket: a key it does not know, a wrong secret, no access
// or no such bucket. Trying again with the same keys cannot help.
func Refused(err error) bool {
	var r minio.ErrorResponse
	if !errors.As(err, &r) {
		return false
	}
	switch r.Code {
	case "AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch", "NoSuchBucket", "AllAccessDisabled":
		return true
	}
	return r.StatusCode == http.StatusUnauthorized || r.StatusCode == http.StatusForbidden
}

// retrier repeats an operation while it fails with a transient error, for at
// most timeout from the first failure. A timeout of 0 tries once.
type retrier struct {
	timeout time.Duration
	// onRetry, if set, is told of each failure that will be retried.
	onRetry func(err error, wait time.Duration)
}

func (r retrier) do(ctx context.Context, op func() error) error {
	err := op()
	if r.timeout <= 0 || !transient(err) {
		return err
	}
	deadline := time.Now().Add(r.timeout)
	wait := minWait
	for transient(err) {
		if time.Now().Add(wait).After(deadline) {
			return fmt.Errorf("%w for %s: %w", ErrUnreachable, r.timeout, err)
		}
		if r.onRetry != nil {
			r.onRetry(err, wait)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
		wait = min(2*wait, maxWait)
		err = op()
	}
	return err
}

// opener opens an object from offset, as long as it is still the version
// etag names; an empty etag means any version. It returns the version it
// opened.
type opener func(ctx context.Context, offset int64, etag string) (io.ReadCloser, string, error)

// resumingReader reads an object and, when the connection breaks in the
// middle, opens it again where it stopped. It only continues from the same
// version, so a reader never gets the start of one version and the end of
// another.
type resumingReader struct {
	ctx    context.Context
	retry  retrier
	open   opener
	etag   string
	offset int64
	body   io.ReadCloser
}

func (r *resumingReader) Read(p []byte) (int, error) {
	n, err := r.body.Read(p)
	r.offset += int64(n)
	if err == nil || err == io.EOF || !transient(err) || r.retry.timeout <= 0 {
		return n, err
	}
	r.body.Close()
	r.body = io.NopCloser(eofReader{})
	if rerr := r.retry.do(r.ctx, func() error {
		body, _, oerr := r.open(r.ctx, r.offset, r.etag)
		if oerr != nil {
			return oerr
		}
		r.body = body
		return nil
	}); rerr != nil {
		return n, rerr
	}
	return n, nil
}

func (r *resumingReader) Close() error { return r.body.Close() }

type eofReader struct{}

func (eofReader) Read([]byte) (int, error) { return 0, io.EOF }
