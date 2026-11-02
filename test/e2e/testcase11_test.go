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

package e2e

import (
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"alpheus-dev.org/alpheus/api/v1alpha1"
	"alpheus-dev.org/alpheus/internal/controller"
)

// TestTestcase11 gives a run a key the storage refuses: the right key ID
// with a wrong secret. The run must fail within two minutes, saying that
// the storage refused the keys, without a single restart and with no pods
// left, and deleting it must not hang on cleaning up data the keys cannot
// delete (GO-137).
func TestTestcase11(t *testing.T) {
	newBench(t, "testcase11", nil) // the sizes the flow needs
	e := setup(t)
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "alpheus-s3-wrong", Namespace: e.ns},
		StringData: map[string]string{
			"AWS_ACCESS_KEY_ID":     need(t, "ALPHEUS_E2E_S3_ACCESS_KEY"),
			"AWS_SECRET_ACCESS_KEY": "not-" + need(t, "ALPHEUS_E2E_S3_SECRET_KEY"),
		},
	}
	if err := e.c.Create(t.Context(), secret); err != nil {
		t.Fatal(err)
	}

	r := e.wait(e.create("testcase11"), 2*time.Minute)
	if r.Status.Phase != v1alpha1.RunFailed || !strings.Contains(r.Status.Message, "the storage refused the keys in Secret alpheus-s3-wrong") {
		t.Fatalf("phase %s: %q, want Failed because the storage refused the keys", r.Status.Phase, r.Status.Message)
	}
	t.Logf("reason: %s", r.Status.Message)
	if r.Status.Reason != v1alpha1.ReasonStorageRefused {
		t.Errorf("reason %q, want %s (GO-138)", r.Status.Reason, v1alpha1.ReasonStorageRefused)
	}
	for _, ss := range r.Status.Steps {
		for _, rs := range ss.Replicas {
			if rs.Restarts != 0 {
				t.Errorf("%s replica %d restarted %d times", ss.Name, rs.Index, rs.Restarts)
			}
		}
	}
	e.eventually("the run's pods deleted", time.Minute, func() bool {
		var list corev1.PodList
		if err := e.c.List(t.Context(), &list, client.InNamespace(e.ns), client.MatchingLabels{controller.LabelRun: r.Name}); err != nil {
			t.Fatal(err)
		}
		return len(list.Items) == 0
	})

	if err := e.c.Delete(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	e.eventually("the run deleted and the Secret let go", time.Minute, func() bool {
		if err := e.c.Get(t.Context(), client.ObjectKeyFromObject(r), &v1alpha1.StreamRun{}); !apierrors.IsNotFound(err) {
			return false
		}
		if err := e.c.Get(t.Context(), client.ObjectKeyFromObject(secret), secret); err != nil {
			t.Fatal(err)
		}
		return len(secret.Finalizers) == 0
	})
}

// eventually checks cond once a second until it is true, and fails the
// test if that takes longer than timeout.
func (e *env) eventually(what string, timeout time.Duration, cond func() bool) {
	e.t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			e.t.Fatalf("not %s within %s", what, timeout)
		}
		time.Sleep(time.Second)
	}
}
