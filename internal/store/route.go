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
	"io"
	"iter"
	"strings"
)

// ResultsPrefix is where results, their run.json, the summary and the error
// log are: results/<run>/<uid>/ (SPEC 5.2).
const ResultsPrefix = "results/"

// WithResults returns a store that keeps keys under ResultsPrefix in
// results and every other key in main, for a run with its own results
// bucket (GO-129).
func WithResults(main, results Store) Store { return &routed{main: main, results: results} }

type routed struct{ main, results Store }

func (r *routed) of(key string) Store {
	if strings.HasPrefix(key, ResultsPrefix) {
		return r.results
	}
	return r.main
}

func (r *routed) Put(ctx context.Context, key string, body io.Reader, size int64) error {
	return r.of(key).Put(ctx, key, body, size)
}

func (r *routed) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	return r.of(key).Get(ctx, key)
}

func (r *routed) GetRange(ctx context.Context, key string, offset int64, etag string) (io.ReadCloser, string, error) {
	return GetRange(ctx, r.of(key), key, offset, etag)
}

func (r *routed) Delete(ctx context.Context, key string) error { return r.of(key).Delete(ctx, key) }

// List lists in the store prefix belongs to. A prefix both stores could
// hold, such as "" or "r", is listed in both, merged in lexical order: the
// results come where ResultsPrefix sorts among the main store's keys.
func (r *routed) List(ctx context.Context, prefix string) iter.Seq2[string, error] {
	if strings.HasPrefix(prefix, ResultsPrefix) {
		return r.results.List(ctx, prefix)
	}
	if !strings.HasPrefix(ResultsPrefix, prefix) {
		return r.main.List(ctx, prefix)
	}
	return func(yield func(string, error) bool) {
		done := false
		results := func() bool {
			done = true
			for key, err := range r.results.List(ctx, prefix) {
				if !yield(key, err) || err != nil {
					return false
				}
			}
			return true
		}
		for key, err := range r.main.List(ctx, prefix) {
			if err == nil && strings.HasPrefix(key, ResultsPrefix) {
				continue
			}
			if !done && err == nil && key > ResultsPrefix && !results() {
				return
			}
			if !yield(key, err) || err != nil {
				return
			}
		}
		if !done {
			results()
		}
	}
}
