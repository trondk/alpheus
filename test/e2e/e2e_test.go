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

// Package e2e runs the testcases (SPEC 12.1) on a Kubernetes cluster with
// the controller installed, normally the Vagrant test cluster through
// hack/e2e.sh. The tests are skipped unless ALPHEUS_E2E=1.
//
// Environment:
//
//	KUBECONFIG                      the cluster
//	BENCH_IMAGE                     the test component, pinned by digest
//	GREP_GO_IMAGE, GREP_PYTHON_IMAGE the component templates (testcase07)
//	METER_*_IMAGE                   the meter-etl example's steps (testcase08)
//	ALPHEUS_E2E_S3_ACCESS_KEY,      the keys put in each run's Secret
//	ALPHEUS_E2E_S3_SECRET_KEY
//	ALPHEUS_E2E_S3_RESULTS_ACCESS_KEY, the key that can only read the
//	ALPHEUS_E2E_S3_RESULTS_SECRET_KEY  bucket alpheus-results (testcase03)
//	ALPHEUS_E2E_S3_ENDPOINT         where this machine reaches the storage,
//	                                instead of the endpoint in status.results,
//	                                which is only reachable in the cluster
//	ALPHEUS_E2E_S3_CA_FILE          CA for that endpoint
//	ALPHEUS_E2E_KEEP=1              keep the namespace of a failed test
//	ALPHEUS_E2E_OUT                 folder for each run's measurements as
//	                                JSON; not set means they are only logged
//
// The testcases also read BENCH_CHUNKS, BENCH_SIZE (bytes per chunk) and
// BENCH_MAX_UNACKED, with defaults 64, 4 MiB and 4, and
// BENCH_<STEP>_REPLICAS for the steps between source and sink: PASS
// (default 1) in testcase01, LEFT and RIGHT (3 and 2) in testcase02 and
// testcase06, FILTER (10) in testcase03, SLOW (2) in testcase05, GO (2)
// in testcase07 and CLEAN (4) in testcase08. testcase08 also reads ETL_FILES
// (4), ETL_ROWS (2000000) per file, so split makes more chunks from one
// file than maxUnacked, ETL_SPLIT_BYTES (16 MiB) and ETL_KILL,
// which is 0 for a run without a killed pod.
// testcase05 reads BENCH_SLOW_MS (500), the slow step's delay per chunk,
// and testcase06 BENCH_CHAOS_SEED, which picks the pods it kills, and
// BENCH_CHAOS_KILLS (12), how many.
package e2e

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/rand"
	k8syaml "k8s.io/apimachinery/pkg/util/yaml"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"alpheus-dev.org/alpheus/api/v1alpha1"
	"alpheus-dev.org/alpheus/internal/chunk"
	"alpheus-dev.org/alpheus/internal/store"
)

// storageTarget is an S3 store the testcases can run against.
type storageTarget struct {
	Name     string
	Endpoint string // as the cluster reaches it
	Region   string
	// The Deployment testcase09 scales to 0 to take the storage away.
	Namespace, Deployment string
	// LateQuota is true for a store that checks a bucket's quota against a
	// usage it counts now and then, so writes over the quota go through
	// until it has counted them (testcase12). MinIO does.
	LateQuota bool
}

// Garage is always in the test cluster (vagrant/services); MinIO only
// while hack/e2e-minio.sh runs.
var (
	garageTarget = storageTarget{Name: "Garage", Endpoint: "http://garage.storage.svc.cluster.local:3900", Region: "garage", Namespace: "storage", Deployment: "garage"}
	minioTarget  = storageTarget{Name: "MinIO", Endpoint: "http://minio.minio.svc.cluster.local:9000", Region: "us-east-1", Namespace: "minio", Deployment: "minio", LateQuota: true}
)

// storage is the store the testcases run against: Garage, or MinIO with
// ALPHEUS_E2E_STORAGE=minio. The flows in testdata name Garage; create
// points them at this one. Both have the same buckets.
func storage(t *testing.T) storageTarget {
	switch v := os.Getenv("ALPHEUS_E2E_STORAGE"); v {
	case "", "garage":
		return garageTarget
	case "minio":
		return minioTarget
	default:
		t.Fatalf("ALPHEUS_E2E_STORAGE=%q: want garage or minio", v)
		return storageTarget{}
	}
}

// env is a test's cluster, namespace and storage.
type env struct {
	t     *testing.T
	c     client.Client
	ns    string
	store store.Store
}

func setup(t *testing.T) *env {
	t.Helper()
	if os.Getenv("ALPHEUS_E2E") != "1" {
		t.Skip("set ALPHEUS_E2E=1 to run against a cluster (hack/e2e.sh)")
	}
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	ctrl.SetLogger(logr.Discard())
	cfg, err := ctrl.GetConfig()
	if err != nil {
		t.Fatal(err)
	}
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	e := &env{t: t, c: c, ns: "alpheus-e2e-" + rand.String(5)}

	ctx := t.Context()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name:   e.ns,
		Labels: map[string]string{"pod-security.kubernetes.io/enforce": "restricted"},
	}}
	if err := c.Create(ctx, ns); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if t.Failed() && os.Getenv("ALPHEUS_E2E_KEEP") == "1" {
			t.Logf("keeping namespace %s", e.ns)
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		c.Delete(ctx, ns)
		// The run's finalizers must let the namespace go, also although
		// its storage Secret is deleted with it (GO-20).
		for {
			if err := c.Get(ctx, client.ObjectKeyFromObject(ns), &corev1.Namespace{}); apierrors.IsNotFound(err) {
				return
			}
			select {
			case <-ctx.Done():
				t.Errorf("namespace %s was not deleted within 3 minutes", e.ns)
				return
			case <-time.After(time.Second):
			}
		}
	})
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "alpheus-s3", Namespace: e.ns},
		StringData: map[string]string{
			"AWS_ACCESS_KEY_ID":     need(t, "ALPHEUS_E2E_S3_ACCESS_KEY"),
			"AWS_SECRET_ACCESS_KEY": need(t, "ALPHEUS_E2E_S3_SECRET_KEY"),
		},
	}
	if err := c.Create(ctx, secret); err != nil {
		t.Fatal(err)
	}
	// A key that can only read the results bucket (GO-129).
	results := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "alpheus-s3-results", Namespace: e.ns},
		StringData: map[string]string{
			"AWS_ACCESS_KEY_ID":     need(t, "ALPHEUS_E2E_S3_RESULTS_ACCESS_KEY"),
			"AWS_SECRET_ACCESS_KEY": need(t, "ALPHEUS_E2E_S3_RESULTS_SECRET_KEY"),
		},
	}
	if err := c.Create(ctx, results); err != nil {
		t.Fatal(err)
	}
	return e
}

func need(t *testing.T, name string) string {
	t.Helper()
	v := os.Getenv(name)
	if v == "" {
		t.Fatalf("%s is not set", name)
	}
	return v
}

// create makes the StreamRun in testdata/<name>.yaml, with ${VAR} replaced
// by environment variables.
func (e *env) create(name string) *v1alpha1.StreamRun {
	e.t.Helper()
	data, err := os.ReadFile("testdata/" + name + ".yaml")
	if err != nil {
		e.t.Fatal(err)
	}
	expanded := os.Expand(string(data), func(v string) string { return need(e.t, v) })
	var r v1alpha1.StreamRun
	if err := k8syaml.NewYAMLOrJSONDecoder(strings.NewReader(expanded), 4096).Decode(&r); err != nil {
		e.t.Fatal(err)
	}
	r.Namespace = e.ns
	if st := storage(e.t); r.Spec.Storage.Endpoint == garageTarget.Endpoint {
		r.Spec.Storage.Endpoint, r.Spec.Storage.Region = st.Endpoint, st.Region
	}
	if err := e.c.Create(e.t.Context(), &r); err != nil {
		e.t.Fatal(err)
	}
	return &r
}

// wait returns the run once it has finished, logging its progress.
func (e *env) wait(r *v1alpha1.StreamRun, timeout time.Duration) *v1alpha1.StreamRun {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(e.t.Context(), timeout)
	defer cancel()
	last := ""
	for {
		if err := e.c.Get(ctx, client.ObjectKeyFromObject(r), r); err != nil {
			e.t.Fatalf("waiting for %s: %v", r.Name, err)
		}
		if now := progress(r); now != last {
			e.t.Log(now)
			last = now
		}
		switch r.Status.Phase {
		case v1alpha1.RunSucceeded, v1alpha1.RunPartiallySucceeded, v1alpha1.RunFailed, v1alpha1.RunCancelled:
			return r
		}
		select {
		case <-ctx.Done():
			e.t.Fatalf("%s did not finish in %s: %s", r.Name, timeout, last)
		case <-time.After(time.Second):
		}
	}
}

func progress(r *v1alpha1.StreamRun) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s %s", r.Status.Phase, r.Status.StepsFinished)
	for _, s := range r.Status.Steps {
		fmt.Fprintf(&b, " %s=%s", s.Name, s.Phase)
	}
	if r.Status.Message != "" {
		fmt.Fprintf(&b, " (%s)", r.Status.Message)
	}
	return b.String()
}

// storeFor opens the storage a result is in, with the keys in the Secret
// the result names (GO-82). Only the endpoint is replaced, when this
// machine reaches the storage another way than the cluster does.
func (e *env) storeFor(res v1alpha1.ResultStatus) store.Store {
	e.t.Helper()
	var secret corev1.Secret
	if err := e.c.Get(e.t.Context(), client.ObjectKey{Namespace: e.ns, Name: res.SecretRef}, &secret); err != nil {
		e.t.Fatal(err)
	}
	endpoint := res.Endpoint
	if v := os.Getenv("ALPHEUS_E2E_S3_ENDPOINT"); v != "" {
		endpoint = v
	}
	s, err := store.NewS3(store.S3Config{
		Endpoint:  endpoint,
		Bucket:    res.Bucket,
		Region:    res.Region,
		AccessKey: string(secret.Data["AWS_ACCESS_KEY_ID"]),
		SecretKey: string(secret.Data["AWS_SECRET_ACCESS_KEY"]),
		CAFile:    os.Getenv("ALPHEUS_E2E_S3_CA_FILE"),
	})
	if err != nil {
		e.t.Fatal(err)
	}
	return s
}

// resultChunks reads every replica's manifest under a result's prefix and
// returns its chunks' content in order.
func resultChunks(t *testing.T, s store.Store, prefix string) []byte {
	t.Helper()
	var all bytes.Buffer
	for key, err := range s.List(t.Context(), prefix) {
		if err != nil {
			t.Fatal(err)
		}
		if path.Base(key) != "manifest.json" {
			continue
		}
		data, err := store.ReadAll(t.Context(), s, key)
		if err != nil {
			t.Fatal(err)
		}
		m, err := chunk.ParseManifest(data)
		if err != nil {
			t.Fatal(err)
		}
		if !m.EOS {
			t.Errorf("%s has no EOS", key)
		}
		for _, c := range m.Chunks {
			data, err := store.ReadAll(t.Context(), s, fmt.Sprintf("%spart-%06d.%s", strings.TrimSuffix(key, "manifest.json"), c.N, c.Ext))
			if err != nil {
				t.Fatal(err)
			}
			all.Write(data)
		}
	}
	return all.Bytes()
}

func stepStatus(t *testing.T, r *v1alpha1.StreamRun, name string) v1alpha1.StepStatus {
	t.Helper()
	for _, s := range r.Status.Steps {
		if s.Name == name {
			return s
		}
	}
	t.Fatalf("no status for step %s", name)
	return v1alpha1.StepStatus{}
}
