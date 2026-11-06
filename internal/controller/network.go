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

package controller

import (
	"context"
	"fmt"
	"net/url"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"alpheus-dev.org/alpheus/api/v1alpha1"
)

// LabelNetwork marks every pod of a run, step pods and replay pods alike,
// with the run's name; the run's NetworkPolicy selects them by it.
const LabelNetwork = "alpheus-dev.org/network"

// RunObjectName is the name of a run's ServiceAccount and NetworkPolicy
// (SPEC 6.1, step 2). Run names are at most 40 characters, so it fits.
func RunObjectName(run string) string { return "alpheus-run-" + run }

// errNotOurs is a ServiceAccount or NetworkPolicy with the run's name that
// the run does not own.
type errNotOurs struct{ kind, name string }

func (e *errNotOurs) Error() string {
	return fmt.Sprintf("%s %s exists and does not belong to the run; delete or rename it", e.kind, e.name)
}

// ensureRunObjects creates the run's ServiceAccount and, unless turned
// off, its NetworkPolicy, and puts the policy right if it has changed
// (SPEC 6.1, step 2). The pods use the ServiceAccount, which has no rights
// and no token, so they cannot use the API server as anyone else in the
// namespace.
func (rc *Reconciler) ensureRunObjects(ctx context.Context, r *v1alpha1.StreamRun) error {
	name := RunObjectName(r.Name)
	sa := &corev1.ServiceAccount{
		ObjectMeta:                   metav1.ObjectMeta{Name: name, Namespace: r.Namespace, Labels: map[string]string{LabelRun: r.Name}},
		AutomountServiceAccountToken: ptr.To(false),
	}
	if err := rc.ensureOwned(ctx, r, sa, &corev1.ServiceAccount{}, nil); err != nil {
		return err
	}
	if rc.Options.NoNetworkPolicy {
		return nil
	}
	np := rc.networkPolicy(r)
	return rc.ensureOwned(ctx, r, np, &networkingv1.NetworkPolicy{}, func(have client.Object) bool {
		h := have.(*networkingv1.NetworkPolicy)
		if equality.Semantic.DeepEqual(h.Spec, np.Spec) {
			return false
		}
		h.Spec = np.Spec
		return true
	})
}

// ensureOwned creates want, owned by the run, if it is missing. If it is
// there, it must be the run's; update, if not nil, changes have to what it
// should be and reports whether it did, and have is then written back.
func (rc *Reconciler) ensureOwned(ctx context.Context, r *v1alpha1.StreamRun, want, have client.Object, update func(have client.Object) bool) error {
	err := rc.Get(ctx, client.ObjectKeyFromObject(want), have)
	if apierrors.IsNotFound(err) {
		if err := controllerutil.SetControllerReference(r, want, rc.Scheme); err != nil {
			return err
		}
		err = rc.Create(ctx, want)
		if apierrors.IsAlreadyExists(err) {
			// The cache had not seen it yet; the next pass checks it.
			return nil
		}
		return err
	}
	if err != nil {
		return err
	}
	if !metav1.IsControlledBy(have, r) {
		kind := "ServiceAccount"
		if _, ok := have.(*networkingv1.NetworkPolicy); ok {
			kind = "NetworkPolicy"
		}
		return &errNotOurs{kind: kind, name: have.GetName()}
	}
	if update != nil && update(have) {
		return rc.Update(ctx, have)
	}
	return nil
}

// networkPolicy lets a run's pods reach only what the agent needs: DNS,
// the storage endpoint's port and the controller's signal hub, and be
// reached only on the agent's metrics port. The pods of a run never talk
// to each other; everything goes through storage. A step that needs more,
// such as a source that reads from a database, gets it from a policy of
// its own: NetworkPolicies add up (docs/isolation.md).
func (rc *Reconciler) networkPolicy(r *v1alpha1.StreamRun) *networkingv1.NetworkPolicy {
	tcp, udp := ptr.To(corev1.ProtocolTCP), ptr.To(corev1.ProtocolUDP)
	port := func(p int32, proto *corev1.Protocol) networkingv1.NetworkPolicyPort {
		return networkingv1.NetworkPolicyPort{Protocol: proto, Port: ptr.To(intstr.FromInt32(p))}
	}

	storage := networkingv1.NetworkPolicyEgressRule{To: rc.Options.StorageEgressTo}
	ports := rc.Options.StoragePorts
	if len(ports) == 0 {
		ports = storagePorts(r)
	}
	for _, p := range ports {
		storage.Ports = append(storage.Ports, port(p, tcp))
	}
	egress := []networkingv1.NetworkPolicyEgressRule{
		{Ports: []networkingv1.NetworkPolicyPort{port(53, udp), port(53, tcp)}},
		storage,
	}
	if rc.Options.HubAddress != "" && rc.Options.HubPort != 0 {
		egress = append(egress, networkingv1.NetworkPolicyEgressRule{
			To: []networkingv1.NetworkPolicyPeer{{
				NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{corev1.LabelMetadataName: rc.Options.Namespace}},
				PodSelector:       &metav1.LabelSelector{MatchLabels: map[string]string{LabelHub: "active"}},
			}},
			Ports: []networkingv1.NetworkPolicyPort{port(rc.Options.HubPort, tcp)},
		})
	}
	return &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: RunObjectName(r.Name), Namespace: r.Namespace, Labels: map[string]string{LabelRun: r.Name}},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{LabelNetwork: r.Name}},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress, networkingv1.PolicyTypeEgress},
			Ingress: []networkingv1.NetworkPolicyIngressRule{{
				Ports: []networkingv1.NetworkPolicyPort{port(AgentMetricsPort, tcp)},
			}},
			Egress: egress,
		},
	}
}

// storagePorts is the port of the run's storage endpoint: the one in its
// URL, or 443 or 80 by its scheme. The port is the one the pod connects
// to; a Service in the cluster whose port differs from its target port
// needs Options.StoragePorts, since a NetworkPolicy sees the target port.
func storagePorts(r *v1alpha1.StreamRun) []int32 {
	u, err := url.Parse(r.Spec.Storage.Endpoint)
	if err != nil {
		return []int32{443}
	}
	if p, err := strconv.ParseInt(u.Port(), 10, 32); err == nil && p > 0 {
		return []int32{int32(p)}
	}
	if u.Scheme == "http" {
		return []int32{80}
	}
	return []int32{443}
}
