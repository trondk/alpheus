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

package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"alpheus-dev.org/alpheus/api/v1alpha1"
	"alpheus-dev.org/alpheus/internal/store"
)

// The annotations on a storage Secret that say where the storage is
// reached from outside the cluster (GO-156). Controller and agents do not
// use them.
const (
	AnnotationExternalEndpoint = "alpheus-dev.org/external-endpoint"
	AnnotationExternalCA       = "alpheus-dev.org/external-ca"
)

// s3Flags are the flags that say where the storage is reached from here.
type s3Flags struct{ endpoint, caFile string }

func (f *s3Flags) add(fs *flag.FlagSet) {
	fs.StringVar(&f.endpoint, "endpoint", "", "the S3 endpoint as seen from here (default: ALPHEUS_S3_ENDPOINT, the storage Secret's "+AnnotationExternalEndpoint+" annotation, or spec.storage.endpoint)")
	fs.StringVar(&f.caFile, "ca-file", "", "CA bundle for the S3 endpoint (default: ALPHEUS_S3_CA_FILE, or the storage Secret's "+AnnotationExternalCA+" annotation)")
}

// storeAddr is where the storage was looked for, and why there.
type storeAddr struct {
	store.Address
	// shown is the endpoint that was tried, from says where it came from.
	shown, from string
	// secret is the Secret whose keys open the storage.
	secret string
}

func (e *Env) getenv(key string) string {
	if e.Getenv != nil {
		return e.Getenv(key)
	}
	return os.Getenv(key)
}

// address finds where the storage is reached from here (GO-156): the
// flags; ALPHEUS_S3_ENDPOINT and ALPHEUS_S3_CA_FILE; the annotations on
// the Secret with the keys, or on spec.storage.secretRef; and last the
// endpoint the run itself uses. The endpoint and the CA are looked up each
// on their own.
func (e *Env) address(ctx context.Context, c client.Reader, r *v1alpha1.StreamRun, f s3Flags, secret string) storeAddr {
	a := storeAddr{secret: secret}
	var annotations map[string]string
	for _, name := range []string{secret, r.Spec.Storage.SecretRef} {
		var s corev1.Secret
		if c.Get(ctx, client.ObjectKey{Namespace: r.Namespace, Name: name}, &s) == nil && s.Annotations[AnnotationExternalEndpoint] != "" {
			annotations = s.Annotations
			break
		}
	}
	switch {
	case f.endpoint != "":
		a.Endpoint, a.from = f.endpoint, "--endpoint"
	case e.getenv("ALPHEUS_S3_ENDPOINT") != "":
		a.Endpoint, a.from = e.getenv("ALPHEUS_S3_ENDPOINT"), "ALPHEUS_S3_ENDPOINT"
	case annotations[AnnotationExternalEndpoint] != "":
		a.Endpoint, a.from = annotations[AnnotationExternalEndpoint], "the annotation "+AnnotationExternalEndpoint
	default:
		a.from = "spec.storage.endpoint, the address inside the cluster"
	}
	a.shown = a.Endpoint
	if a.shown == "" {
		a.shown = r.Spec.Storage.Endpoint
	}
	switch {
	case f.caFile != "":
		a.CAFile = f.caFile
	case e.getenv("ALPHEUS_S3_CA_FILE") != "":
		a.CAFile = e.getenv("ALPHEUS_S3_CA_FILE")
	case annotations[AnnotationExternalCA] != "":
		a.CAPEM = []byte(annotations[AnnotationExternalCA])
	}
	return a
}

// explain says, for an error from the storage, which address was tried
// and what can be set instead (GO-156).
func (a storeAddr) explain(err error) error {
	if err == nil {
		return nil
	}
	var he *hintError
	if errors.As(err, &he) || errors.Is(err, context.Canceled) {
		return err
	}
	return hint(fmt.Errorf("storage at %s (from %s): %w", a.shown, a.from, err),
		"if that address is only reachable inside the cluster, pass the one you reach it by with --endpoint and --ca-file, "+
			"set ALPHEUS_S3_ENDPOINT and ALPHEUS_S3_CA_FILE, or annotate the Secret "+a.secret+" with "+
			AnnotationExternalEndpoint+" and "+AnnotationExternalCA)
}

// openRun opens the run's storage with the keys in spec.storage.secretRef,
// at the address seen from here.
func (e *Env) openRun(ctx context.Context, c client.Reader, r *v1alpha1.StreamRun, f s3Flags) (store.Store, storeAddr, error) {
	a := e.address(ctx, c, r, f, r.Spec.Storage.SecretRef)
	st, err := e.OpenStore(ctx, c, r, a.Address)
	if err != nil {
		return nil, a, hint(err, "the storage is read with the keys in the Secret "+r.Spec.Storage.SecretRef+", so you need access to it")
	}
	return st, a, nil
}

// resultsAccess is the bucket and Secret a run's results are read with:
// those in status.results, which name the key that can only read the
// results bucket when the run has one (GO-129), or else spec.storage's.
func resultsAccess(r *v1alpha1.StreamRun) (bucket, secret string) {
	for _, res := range r.Status.Results {
		if res.Bucket != "" && res.SecretRef != "" {
			return res.Bucket, res.SecretRef
		}
	}
	bucket, secret = r.Spec.Storage.Bucket, r.Spec.Storage.SecretRef
	if r.Spec.Storage.ResultsBucket != "" {
		bucket = r.Spec.Storage.ResultsBucket
	}
	if r.Spec.Storage.ResultsSecretRef != "" {
		secret = r.Spec.Storage.ResultsSecretRef
	}
	return bucket, secret
}

// openResults opens the bucket with the run's results, with the keys
// status.results names, at the address seen from here. Everything under
// results/<run>/<uid>/ is there: the results, the error log and the kept
// logs and failures.
func (e *Env) openResults(ctx context.Context, c client.Reader, r *v1alpha1.StreamRun, f s3Flags) (store.Store, storeAddr, string, error) {
	bucket, name := resultsAccess(r)
	a := e.address(ctx, c, r, f, name)
	var s corev1.Secret
	if err := c.Get(ctx, client.ObjectKey{Namespace: r.Namespace, Name: name}, &s); err != nil {
		if isNotFound(err) {
			return nil, a, "", fmt.Errorf("secret %s with the keys to the results not found in namespace %s", name, r.Namespace)
		}
		return nil, a, "", hint(apiError(err), "the results are read with the keys in the Secret "+name+", so you need access to it")
	}
	endpoint := a.Endpoint
	if endpoint == "" {
		endpoint = r.Spec.Storage.Endpoint
	}
	st, err := e.NewS3(store.S3Config{
		Endpoint:  endpoint,
		Bucket:    bucket,
		Region:    r.Spec.Storage.Region,
		AccessKey: string(s.Data["AWS_ACCESS_KEY_ID"]),
		SecretKey: string(s.Data["AWS_SECRET_ACCESS_KEY"]),
		CAFile:    a.CAFile,
		CAPEM:     a.CAPEM,
	})
	if err != nil {
		return nil, a, "", a.explain(err)
	}
	return st, a, bucket, nil
}
