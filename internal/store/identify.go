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
	"net/http"
	"strings"
)

// Tested are the stores Alpheus is tested with, which have the strong
// read-after-write consistency it needs (GO-69).
var Tested = []string{"AWS S3", "Garage", "MinIO"}

// Identify asks the endpoint which store it is, with one request without
// keys, and returns one of Tested, or "" for a store it does not know.
// AWS S3 and MinIO name themselves in the Server header, and Garage in
// the error it gives a request without keys.
func Identify(ctx context.Context, cfg S3Config) (string, error) {
	t, err := transport(cfg.CAFile)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimSuffix(cfg.Endpoint, "/")+"/", nil)
	if err != nil {
		return "", fmt.Errorf("s3 endpoint %q: %w", cfg.Endpoint, err)
	}
	resp, err := (&http.Client{Transport: t}).Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	server := resp.Header.Get("Server")
	switch {
	case strings.HasPrefix(server, "AmazonS3"):
		return "AWS S3", nil
	case strings.Contains(server, "MinIO"):
		return "MinIO", nil
	case strings.Contains(server, "Garage") || strings.Contains(string(body), "Garage"):
		return "Garage", nil
	}
	return "", nil
}
