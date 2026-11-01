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
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
)

func fastBackoff(t *testing.T) {
	oldMin, oldMax := minWait, maxWait
	minWait, maxWait = time.Millisecond, 4*time.Millisecond
	t.Cleanup(func() { minWait, maxWait = oldMin, oldMax })
}

func TestTransient(t *testing.T) {
	refused := &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
	for _, c := range []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"connection refused", fmt.Errorf("put x: %w", refused), true},
		{"reset", syscall.ECONNRESET, true},
		{"cut off body", io.ErrUnexpectedEOF, true},
		{"503", minio.ErrorResponse{StatusCode: http.StatusServiceUnavailable, Code: "ServiceUnavailable"}, true},
		{"500", minio.ErrorResponse{StatusCode: http.StatusInternalServerError, Code: "InternalError"}, true},
		{"429", minio.ErrorResponse{StatusCode: http.StatusTooManyRequests, Code: "SlowDown"}, true},
		{"access denied", minio.ErrorResponse{StatusCode: http.StatusForbidden, Code: "AccessDenied"}, false},
		{"wrong key", minio.ErrorResponse{StatusCode: http.StatusForbidden, Code: "InvalidAccessKeyId"}, false},
		{"missing", minio.ErrorResponse{StatusCode: http.StatusNotFound, Code: "NoSuchKey"}, false},
		{"changed", minio.ErrorResponse{StatusCode: http.StatusPreconditionFailed, Code: "PreconditionFailed"}, false},
		{"cancelled", context.Canceled, false},
		{"other", errors.New("bad input"), false},
	} {
		if got := transient(c.err); got != c.want {
			t.Errorf("%s: transient = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestRetryUntilBack(t *testing.T) {
	fastBackoff(t)
	calls, retries := 0, 0
	r := retrier{timeout: time.Second, onRetry: func(error, time.Duration) { retries++ }}
	err := r.do(context.Background(), func() error {
		if calls++; calls < 4 {
			return syscall.ECONNREFUSED
		}
		return nil
	})
	if err != nil || calls != 4 || retries != 3 {
		t.Fatalf("err %v after %d calls and %d retries, want nil after 4 and 3", err, calls, retries)
	}
}

func TestRetryGivesUp(t *testing.T) {
	fastBackoff(t)
	r := retrier{timeout: 20 * time.Millisecond}
	start := time.Now()
	err := r.do(context.Background(), func() error { return syscall.ECONNREFUSED })
	if !errors.Is(err, ErrUnreachable) || !errors.Is(err, syscall.ECONNREFUSED) {
		t.Fatalf("err %v, want ErrUnreachable wrapping the last error", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("gave up after %s", d)
	}
}

func TestRetryNotPermanentErrors(t *testing.T) {
	fastBackoff(t)
	calls := 0
	denied := minio.ErrorResponse{StatusCode: http.StatusForbidden, Code: "AccessDenied"}
	err := retrier{timeout: time.Second}.do(context.Background(), func() error { calls++; return denied })
	if calls != 1 || errors.Is(err, ErrUnreachable) {
		t.Fatalf("%d calls, err %v: a refused request must fail at once", calls, err)
	}
	calls = 0
	retrier{}.do(context.Background(), func() error { calls++; return syscall.ECONNREFUSED })
	if calls != 1 {
		t.Fatalf("timeout 0 made %d calls, want 1", calls)
	}
}

func TestRetryStopsOnCancel(t *testing.T) {
	fastBackoff(t)
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	err := retrier{timeout: time.Minute}.do(ctx, func() error {
		if calls++; calls == 2 {
			cancel()
		}
		return syscall.ECONNREFUSED
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err %v, want context.Canceled", err)
	}
}

// breaking is a body that fails with a transient error after n bytes.
type breaking struct {
	r *bytes.Reader
	n int
}

func (b *breaking) Read(p []byte) (int, error) {
	if b.n <= 0 {
		return 0, io.ErrUnexpectedEOF
	}
	if len(p) > b.n {
		p = p[:b.n]
	}
	n, err := b.r.Read(p)
	b.n -= n
	return n, err
}

func (b *breaking) Close() error { return nil }

// object is an object whose version can change, opened through an opener
// whose bodies break off after every 7 bytes.
type object struct {
	data, etag string
	opens      []string
}

func (o *object) open(_ context.Context, offset int64, etag string) (io.ReadCloser, string, error) {
	o.opens = append(o.opens, fmt.Sprintf("%d@%s", offset, etag))
	if etag != "" && etag != o.etag {
		return nil, "", minio.ErrorResponse{StatusCode: http.StatusPreconditionFailed, Code: "PreconditionFailed"}
	}
	return &breaking{r: bytes.NewReader([]byte(o.data[offset:])), n: 7}, o.etag, nil
}

func TestResumingReader(t *testing.T) {
	fastBackoff(t)
	o := &object{data: "the river runs under the limestone", etag: "v1"}
	body, etag, _ := o.open(context.Background(), 0, "")
	r := &resumingReader{ctx: context.Background(), retry: retrier{timeout: time.Second}, open: o.open, etag: etag, body: body}
	got, err := io.ReadAll(r)
	if err != nil || string(got) != o.data {
		t.Fatalf("read %q, %v; want %q", got, err, o.data)
	}
	if want := "0@,7@v1,14@v1,21@v1,28@v1"; strings.Join(o.opens, ",") != want {
		t.Errorf("opens %s, want %s", strings.Join(o.opens, ","), want)
	}
}

func TestResumingReaderSameVersionOnly(t *testing.T) {
	fastBackoff(t)
	o := &object{data: "first version of the manifest", etag: "v1"}
	body, etag, _ := o.open(context.Background(), 0, "")
	r := &resumingReader{ctx: context.Background(), retry: retrier{timeout: time.Second}, open: o.open, etag: etag, body: body}
	o.etag = "v2" // overwritten while it was read
	got, err := io.ReadAll(r)
	if err == nil {
		t.Fatalf("read %q of a changed object without an error", got)
	}
	if string(got) != o.data[:7] {
		t.Errorf("read %q before the error, want %q", got, o.data[:7])
	}
}

func TestResumingReaderWithoutTimeout(t *testing.T) {
	o := &object{data: "no retries at all", etag: "v1"}
	body, etag, _ := o.open(context.Background(), 0, "")
	r := &resumingReader{ctx: context.Background(), open: o.open, etag: etag, body: body}
	if _, err := io.ReadAll(r); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("err %v, want the read error itself", err)
	}
}
