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
	"os"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"alpheus-dev.org/alpheus/api/v1alpha1"
	"alpheus-dev.org/alpheus/internal/controller"
)

// TestTestcase20 checks what a run's pods can reach: its NetworkPolicy and
// ServiceAccount (SPEC 6.1, step 2).
//
// Flow: source → sink, run twice. source tries the Kubernetes API, Garage's
// admin port and Zot before it writes, and counts what it reached.
//
// What it tests: that a run's policy blocks everything but the storage and
// the signal hub, that its ServiceAccount has no token, and that a policy of
// the user's own adds to it, as docs/isolation.md shows.
//
// Done to the run: before the second run, the test adds a policy that lets
// source reach Zot.
//
// Passes when: both runs are Succeeded; in the first, all three were
// blocked; in the second, Zot was reached and the other two blocked; and the
// run owns its NetworkPolicy and ServiceAccount, which does not mount a
// token. It needs a network plugin that enforces NetworkPolicies;
// hack/e2e.sh sets ALPHEUS_E2E_NETWORK_POLICY=0 on a cluster with flannel.
func TestTestcase20(t *testing.T) {
	if os.Getenv("ALPHEUS_E2E_NETWORK_POLICY") == "0" {
		t.Skip("the cluster's network does not enforce NetworkPolicies")
	}
	e := setup(t)
	r := e.wait(e.create("testcase20"), 5*time.Minute)
	if r.Status.Phase != v1alpha1.RunSucceeded {
		t.Fatalf("phase %s: %s", r.Status.Phase, r.Status.Message)
	}
	checkRunObjects(t, e, r)
	counts := stepStatus(t, r, "source").Counts
	t.Logf("source: %d reached, %d blocked, %d unresolved", counts["reached"], counts["blocked"], counts["unresolved"])
	if counts["reached"] != 0 || counts["blocked"] != 3 || counts["unresolved"] != 0 {
		t.Errorf("counts %v, want all 3 blocked", counts)
	}

	// The test's own policy: source may also reach Zot.
	zot := &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "source-to-zot", Namespace: e.ns},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{controller.LabelStep: "source"}},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeEgress},
			Egress: []networkingv1.NetworkPolicyEgressRule{{
				To: []networkingv1.NetworkPolicyPeer{{
					NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{corev1.LabelMetadataName: "registry"}},
				}},
				Ports: []networkingv1.NetworkPolicyPort{{Protocol: ptr.To(corev1.ProtocolTCP), Port: ptr.To(intstr.FromInt32(5000))}},
			}},
		},
	}
	if err := e.c.Create(t.Context(), zot); err != nil {
		t.Fatal(err)
	}
	again := e.load("testcase20")
	again.Name = "testcase20-zot"
	if err := e.c.Create(t.Context(), again); err != nil {
		t.Fatal(err)
	}
	again = e.wait(again, 5*time.Minute)
	if again.Status.Phase != v1alpha1.RunSucceeded {
		t.Fatalf("phase %s: %s", again.Status.Phase, again.Status.Message)
	}
	counts = stepStatus(t, again, "source").Counts
	t.Logf("with the extra policy, source: %d reached, %d blocked, %d unresolved", counts["reached"], counts["blocked"], counts["unresolved"])
	if counts["reached"] != 1 || counts["blocked"] != 2 || counts["unresolved"] != 0 {
		t.Errorf("counts %v, want Zot reached and the other 2 blocked", counts)
	}
}

// checkRunObjects checks that the run's ServiceAccount and NetworkPolicy
// are its own, and that its pods used them.
func checkRunObjects(t *testing.T, e *env, r *v1alpha1.StreamRun) {
	t.Helper()
	key := client.ObjectKey{Namespace: e.ns, Name: controller.RunObjectName(r.Name)}
	var sa corev1.ServiceAccount
	if err := e.c.Get(t.Context(), key, &sa); err != nil {
		t.Fatal(err)
	}
	if !metav1.IsControlledBy(&sa, r) || sa.AutomountServiceAccountToken == nil || *sa.AutomountServiceAccountToken {
		t.Errorf("ServiceAccount %s: owners %v, automount %v", sa.Name, sa.OwnerReferences, sa.AutomountServiceAccountToken)
	}
	var np networkingv1.NetworkPolicy
	if err := e.c.Get(t.Context(), key, &np); err != nil {
		t.Fatal(err)
	}
	if !metav1.IsControlledBy(&np, r) {
		t.Errorf("NetworkPolicy %s: owners %v", np.Name, np.OwnerReferences)
	}
}
