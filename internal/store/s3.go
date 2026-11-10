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
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"iter"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// S3Config says how to reach a bucket. It comes from spec.storage and the
// Secret it names.
type S3Config struct {
	// Endpoint is the URL of the S3 API, http:// or https://.
	Endpoint  string
	Bucket    string
	Region    string
	AccessKey string
	SecretKey string
	// CAFile is a PEM file of CA certificates to trust for https, in
	// addition to the system's. Empty means the system's only.
	CAFile string
	// CAPEM are more CA certificates to trust, as PEM, such as the
	// alpheus-dev.org/external-ca annotation (GO-156).
	CAPEM []byte
	// Timeout is how long a call keeps trying while the storage is down or
	// answers with a temporary error, before it fails with ErrUnreachable
	// (GO-136). 0 tries once, apart from the client's own few retries.
	Timeout time.Duration
	// OnRetry, if set, is told of each failure that will be retried.
	OnRetry func(err error, wait time.Duration)
}

// Address is where a program reaches the storage from, when that is not
// the address the cluster uses (GO-156). Empty fields keep spec.storage's
// endpoint and the system's CAs.
type Address struct {
	Endpoint string
	CAFile   string
	CAPEM    []byte
}

// S3 is a Store in an S3-compatible bucket.
type S3 struct {
	client *minio.Client
	bucket string
	retry  retrier
}

// NewS3 connects to the bucket. It makes no request; the first call does.
func NewS3(cfg S3Config) (*S3, error) {
	u, err := url.Parse(cfg.Endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("s3 endpoint %q: want http(s)://host[:port]", cfg.Endpoint)
	}
	if u.Path != "" && u.Path != "/" {
		return nil, fmt.Errorf("s3 endpoint %q: must not have a path", cfg.Endpoint)
	}
	transport, err := transport(cfg.CAFile, cfg.CAPEM)
	if err != nil {
		return nil, err
	}
	client, err := minio.New(u.Host, &minio.Options{
		Creds:     credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure:    u.Scheme == "https",
		Region:    cfg.Region,
		Transport: transport,
		// Path-style requests (endpoint/bucket/key) work with every
		// S3-compatible store and need no DNS per bucket.
		BucketLookup: minio.BucketLookupPath,
	})
	if err != nil {
		return nil, fmt.Errorf("s3 client: %w", err)
	}
	return &S3{client: client, bucket: cfg.Bucket, retry: retrier{timeout: cfg.Timeout, onRetry: cfg.OnRetry}}, nil
}

func transport(caFile string, caPEM []byte) (http.RoundTripper, error) {
	t, err := minio.DefaultTransport(true)
	if err != nil {
		return nil, fmt.Errorf("s3 transport: %w", err)
	}
	if caFile == "" && len(caPEM) == 0 {
		return t, nil
	}
	pool, err := x509.SystemCertPool()
	if err != nil {
		pool = x509.NewCertPool()
	}
	if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("s3 CA file: %w", err)
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("s3 CA file %s: no certificates", caFile)
		}
	}
	if len(caPEM) > 0 && !pool.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("s3 CA: no certificates in the PEM")
	}
	if t.TLSClientConfig == nil {
		t.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	t.TLSClientConfig.RootCAs = pool
	return t, nil
}

func (s *S3) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	put := func() error {
		_, err := s.client.PutObject(ctx, s.bucket, key, r, size, minio.PutObjectOptions{})
		return err
	}
	// A body that can be rewound can be sent again; overwriting a key is
	// safe to repeat, since every key has one writer (GO-68).
	retry := retrier{}
	if seeker, ok := r.(io.Seeker); ok {
		if start, err := seeker.Seek(0, io.SeekCurrent); err == nil {
			retry = s.retry
			first := true
			send := put
			put = func() error {
				if !first {
					if _, err := seeker.Seek(start, io.SeekStart); err != nil {
						return err
					}
				}
				first = false
				return send()
			}
		}
	}
	if err := retry.do(ctx, put); err != nil {
		return fmt.Errorf("put %s: %w", key, err)
	}
	return nil
}

func (s *S3) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	body, _, err := s.GetRange(ctx, key, 0, "")
	return body, err
}

// GetRange opens key from offset, as long as it is the version etag names
// (GO-149). It makes one GET whose body is the object. Client.GetObject
// would make a second request with If-Match on the first one's ETag, which
// fails when a manifest is overwritten in between.
func (s *S3) GetRange(ctx context.Context, key string, offset int64, etag string) (io.ReadCloser, string, error) {
	open := func(ctx context.Context, offset int64, etag string) (io.ReadCloser, string, error) {
		var opts minio.GetObjectOptions
		if offset > 0 {
			if err := opts.SetRange(offset, 0); err != nil {
				return nil, "", err
			}
		}
		if etag != "" {
			if err := opts.SetMatchETag(etag); err != nil {
				return nil, "", err
			}
		}
		body, info, _, err := minio.Core{Client: s.client}.GetObject(ctx, s.bucket, key, opts)
		return body, info.ETag, err
	}
	var body io.ReadCloser
	err := s.retry.do(ctx, func() (err error) {
		var got string
		body, got, err = open(ctx, offset, etag)
		if etag == "" {
			etag = got
		}
		return err
	})
	if err != nil {
		return nil, "", fmt.Errorf("get %s: %w", key, mapErr(err))
	}
	return &resumingReader{ctx: ctx, retry: s.retry, open: open, etag: etag, offset: offset, body: body}, etag, nil
}

// Size is the size of key, from its metadata.
func (s *S3) Size(ctx context.Context, key string) (int64, error) {
	var info minio.ObjectInfo
	err := s.retry.do(ctx, func() (err error) {
		info, err = s.client.StatObject(ctx, s.bucket, key, minio.StatObjectOptions{})
		return err
	})
	if err != nil {
		return 0, fmt.Errorf("stat %s: %w", key, mapErr(err))
	}
	return info.Size, nil
}

func (s *S3) Delete(ctx context.Context, key string) error {
	if err := s.retry.do(ctx, func() error {
		return s.client.RemoveObject(ctx, s.bucket, key, minio.RemoveObjectOptions{})
	}); err != nil {
		if errors.Is(mapErr(err), ErrNotFound) {
			return nil
		}
		return fmt.Errorf("delete %s: %w", key, err)
	}
	return nil
}

func (s *S3) List(ctx context.Context, prefix string) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		// The listing stops when ctx is done, so leaving the loop early
		// must cancel it.
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		// A listing that breaks off goes on after the last key it gave.
		last := ""
		page := func() error {
			opts := minio.ListObjectsOptions{Prefix: prefix, Recursive: true, StartAfter: last}
			for obj := range s.client.ListObjects(ctx, s.bucket, opts) {
				if obj.Err != nil {
					return obj.Err
				}
				last = obj.Key
				if !yield(obj.Key, nil) {
					return errStop
				}
			}
			return ctx.Err()
		}
		if err := s.retry.do(ctx, page); err != nil && err != errStop {
			yield("", fmt.Errorf("list %s: %w", prefix, err))
		}
	}
}

// errStop ends a listing the caller left early.
var errStop = errors.New("stop")

func mapErr(err error) error {
	switch minio.ToErrorResponse(err).Code {
	case minio.NoSuchKey:
		return fmt.Errorf("%w: %w", ErrNotFound, err)
	case "PreconditionFailed":
		return fmt.Errorf("%w: %w", ErrChanged, err)
	}
	return err
}
