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

// Command api-client uses Alpheus the way any program can, through the
// Kubernetes API and S3 alone, as docs/api.md describes: it creates a
// StreamRun from a flow file, waits for it to finish, and downloads each
// result, checking every chunk against its size and SHA-256. It uses the
// API types in alpheus-dev.org/alpheus/api/v1alpha1 and nothing else from
// Alpheus.
//
// Usage:
//
//	api-client -f flow.yaml [-n namespace] [-out dir] [-endpoint URL] [-ca-file file]
//
// -endpoint replaces the storage's address in status.results, for a
// program outside the cluster that reaches the storage by another name.
package main

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	"alpheus-dev.org/alpheus/api/v1alpha1"
)

func main() {
	file := flag.String("f", "", "the flow: a StreamRun in YAML")
	ns := flag.String("n", "default", "the namespace to run it in")
	out := flag.String("out", "results", "where to put the results")
	endpoint := flag.String("endpoint", "", "the storage's address from here, instead of the one in status.results")
	caFile := flag.String("ca-file", "", "a CA for the storage's certificate")
	flag.Parse()
	if *file == "" {
		fmt.Fprintln(os.Stderr, "usage: api-client -f flow.yaml [-n namespace] [-out dir] [-endpoint URL] [-ca-file file]")
		os.Exit(2)
	}
	if err := run(context.Background(), *file, *ns, *out, *endpoint, *caFile); err != nil {
		fmt.Fprintln(os.Stderr, "api-client:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, file, ns, out, endpoint, caFile string) error {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return err
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		return err
	}
	// The kubeconfig, as kubectl finds it, or the pod's ServiceAccount.
	cfg, err := ctrl.GetConfig()
	if err != nil {
		return err
	}
	k8s, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		return err
	}

	// 1. Start the run: create the StreamRun.
	data, err := os.ReadFile(file)
	if err != nil {
		return err
	}
	var r v1alpha1.StreamRun
	if err := yaml.UnmarshalStrict(data, &r); err != nil {
		return fmt.Errorf("%s: %w", file, err)
	}
	r.Namespace = ns
	if err := k8s.Create(ctx, &r); err != nil {
		return err
	}
	fmt.Printf("created %s/%s\n", ns, r.Name)

	// 2. Follow it: the run has finished when Progressing is False.
	last := ""
	for {
		if err := k8s.Get(ctx, client.ObjectKeyFromObject(&r), &r); err != nil {
			return err
		}
		if now := fmt.Sprintf("%s %s", r.Status.Phase, r.Status.StepsFinished); r.Status.Phase != "" && now != last {
			fmt.Println(now)
			last = now
		}
		if c := meta.FindStatusCondition(r.Status.Conditions, v1alpha1.ConditionProgressing); c != nil && c.Status == "False" {
			break
		}
		time.Sleep(2 * time.Second)
	}
	switch r.Status.Phase {
	case v1alpha1.RunSucceeded:
	case v1alpha1.RunPartiallySucceeded:
		fmt.Printf("partly: %s: %s\n", r.Status.Reason, r.Status.Message)
	default:
		return fmt.Errorf("the run %s: %s: %s", r.Status.Phase, r.Status.Reason, r.Status.Message)
	}

	// 3. Fetch the results.
	for _, res := range r.Status.Results {
		replicas := 1
		for _, s := range r.Spec.Steps {
			if s.Name == res.Step && s.Replicas != nil {
				replicas = int(*s.Replicas)
			}
		}
		var key corev1.Secret
		if err := k8s.Get(ctx, client.ObjectKey{Namespace: ns, Name: res.SecretRef}, &key); err != nil {
			return err
		}
		ep := res.Endpoint
		if endpoint != "" {
			ep = endpoint
		}
		s3, err := connect(ep, res.Region, caFile,
			string(key.Data["AWS_ACCESS_KEY_ID"]), string(key.Data["AWS_SECRET_ACCESS_KEY"]))
		if err != nil {
			return err
		}
		for i := range replicas {
			dir := filepath.Join(out, res.Step+"."+res.Output, fmt.Sprint(i))
			n, err := fetch(ctx, s3, res.Bucket, fmt.Sprintf("%s%d/", res.Prefix, i), dir)
			if err != nil {
				return fmt.Errorf("%s.%s replica %d: %w", res.Step, res.Output, i, err)
			}
			fmt.Printf("%s.%s replica %d: %d chunks in %s\n", res.Step, res.Output, i, n, dir)
		}
	}
	return nil
}

// manifest is the part of a stream's manifest.json a reader needs. A
// sealed segment, manifest-0000.json and so on, has the chunks before the
// ones in manifest.json, in the same form.
type manifest struct {
	Segments []string `json:"segments"`
	Chunks   []chunk  `json:"chunks"`
	EOS      bool     `json:"eos"`
}

type chunk struct {
	N      int64  `json:"n"`
	Ext    string `json:"ext"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

// fetch downloads the chunks one replica's manifest lists, and only
// those: an object that is not in the manifest is not committed.
func fetch(ctx context.Context, s3 *minio.Client, bucket, prefix, dir string) (int, error) {
	var m manifest
	if err := getJSON(ctx, s3, bucket, prefix+"manifest.json", &m); err != nil {
		return 0, err
	}
	if !m.EOS {
		return 0, errors.New("the result has not ended")
	}
	var chunks []chunk
	for _, seg := range m.Segments {
		var s manifest
		if err := getJSON(ctx, s3, bucket, prefix+seg, &s); err != nil {
			return 0, err
		}
		chunks = append(chunks, s.Chunks...)
	}
	chunks = append(chunks, m.Chunks...)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return 0, err
	}
	for _, c := range chunks {
		name := fmt.Sprintf("part-%06d.%s", c.N, c.Ext)
		if err := download(ctx, s3, bucket, prefix+name, filepath.Join(dir, name), c); err != nil {
			return 0, err
		}
	}
	return len(chunks), nil
}

// download writes a chunk to path and checks it against the manifest.
func download(ctx context.Context, s3 *minio.Client, bucket, key, path string, c chunk) error {
	obj, err := s3.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return err
	}
	defer obj.Close()
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(f, h), obj)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return fmt.Errorf("%s: %w", key, err)
	}
	if n != c.Bytes || hex.EncodeToString(h.Sum(nil)) != c.SHA256 {
		os.Remove(path)
		return fmt.Errorf("%s: %d bytes with another SHA-256 than the manifest's %d bytes", key, n, c.Bytes)
	}
	return nil
}

func getJSON(ctx context.Context, s3 *minio.Client, bucket, key string, v any) error {
	obj, err := s3.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return err
	}
	defer obj.Close()
	data, err := io.ReadAll(obj)
	if err != nil {
		return fmt.Errorf("%s: %w", key, err)
	}
	return json.Unmarshal(data, v)
}

// connect opens the storage with path-style requests, which every
// S3-compatible store understands.
func connect(endpoint, region, caFile, accessKey, secretKey string) (*minio.Client, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" {
		return nil, fmt.Errorf("endpoint %q: want http(s)://host[:port]", endpoint)
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("%s: no certificates", caFile)
		}
		tr.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	}
	return minio.New(u.Host, &minio.Options{
		Creds:        credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure:       u.Scheme == "https",
		Region:       region,
		Transport:    tr,
		BucketLookup: minio.BucketLookupPath,
	})
}
