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
	"net/http"
	"net/http/httptest"
	"testing"
)

// Identify knows the tested stores by what they answer a request without
// keys, and no other (GO-69).
func TestIdentify(t *testing.T) {
	for _, c := range []struct {
		server, body, want string
	}{
		{"AmazonS3", "<Error><Code>AccessDenied</Code></Error>", "AWS S3"},
		{"MinIO", "<Error><Code>AccessDenied</Code></Error>", "MinIO"},
		// As Garage v2 answers.
		{"", "<Error><Code>AccessDenied</Code><Message>Forbidden: Garage does not support anonymous access yet</Message><Resource>/</Resource><Region>garage</Region></Error>", "Garage"},
		{"Ceph Object Gateway", "<Error><Code>AccessDenied</Code></Error>", ""},
		{"", "", ""},
	} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "" {
				t.Error("Identify sent keys")
			}
			if c.server != "" {
				w.Header().Set("Server", c.server)
			}
			w.WriteHeader(http.StatusForbidden)
			w.Write([]byte(c.body))
		}))
		got, err := Identify(context.Background(), S3Config{Endpoint: srv.URL})
		srv.Close()
		if err != nil || got != c.want {
			t.Errorf("server %q: %q, %v; want %q", c.server, got, err, c.want)
		}
	}
	if _, err := Identify(context.Background(), S3Config{Endpoint: "http://127.0.0.1:1"}); err == nil {
		t.Error("no error for an endpoint that does not answer")
	}
}
