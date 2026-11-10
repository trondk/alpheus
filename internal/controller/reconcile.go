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

// Package controller reconciles StreamRuns (SPEC 6): it validates the
// graph, runs one pod per replica of each step with the agent as a native
// sidecar, restarts failed attempts, and keeps status up to date from the
// pods and from what the agents committed in storage.
package controller

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"alpheus-dev.org/alpheus/api/v1alpha1"
	"alpheus-dev.org/alpheus/internal/chunk"
	"alpheus-dev.org/alpheus/internal/store"
)

// Finalizer makes sure a run's storage prefix is deleted before the run
// disappears (GO-20).
const Finalizer = "alpheus-dev.org/cleanup"

// secretFinalizer keeps a run's storage Secret until the run's prefix is
// deleted: deleting a namespace deletes the Secret and the run together,
// and the cleanup needs the keys (GO-20). It is one per run, so runs that
// share a Secret each hold it.
func secretFinalizer(r *v1alpha1.StreamRun) string {
	return "alpheus-dev.org/storage-" + string(r.UID)
}

// agentRestartLimit is how often the agent sidecar may restart in place
// before the attempt counts as failed.
const agentRestartLimit = 3

// Options configure the controller.
type Options struct {
	// AgentImage is the agent's image, pinned by digest; all steps use it
	// (GO-24).
	AgentImage string
	// AllowTags lets component images be given without a digest, for
	// development (GO-26).
	AllowTags bool
	// HubAddress is where agents reach the signal hub, such as
	// "alpheus-signal-hub.alpheus-system.svc:7070". Empty means agents
	// only poll.
	HubAddress string
	// Namespace is the controller's namespace, which holds the signal CA.
	Namespace string
	// NewStore opens a run's storage; nil means S3 with the run's Secret.
	NewStore func(ctx context.Context, r *v1alpha1.StreamRun) (store.Store, error)
	// StorageCAFile is a CA bundle for storage endpoints, or empty.
	StorageCAFile string
	// NoNetworkPolicy turns off the run's NetworkPolicy (SPEC 6.1, step
	// 2), for a cluster whose network does not enforce them or that has
	// its own.
	NoNetworkPolicy bool
	// HubPort is the port the signal hub listens on, which the run's
	// NetworkPolicy lets agents reach.
	HubPort int32
	// StoragePorts are the ports the run's pods may reach the storage on;
	// empty means the port of the run's endpoint.
	StoragePorts []int32
	// StorageEgressTo narrows where the storage is, such as a namespace or
	// an address range; empty means any address.
	StorageEgressTo []networkingv1.NetworkPolicyPeer
	// Resync is how often a running run's status is read again from
	// storage; default 2s.
	Resync time.Duration
	// StatusInterval is the least time between two status writes of one
	// run; default 1s (GO-22).
	StatusInterval time.Duration
}

// Reconciler reconciles StreamRuns.
type Reconciler struct {
	client.Client
	// APIReader reads from the API server instead of the cache, to confirm
	// that a pod is gone (GO-67). Nil uses Client.
	APIReader client.Reader
	Scheme    *runtime.Scheme
	Options   Options

	// Recorder writes the run's Kubernetes events (GO-139); nil writes
	// none.
	Recorder events.EventRecorder
	// Logs reads a container's log, for keeping the logs of failed
	// attempts (GO-140); nil keeps none.
	Logs func(ctx context.Context, namespace, pod, container string, previous bool) (io.ReadCloser, error)

	caOnce sync.Mutex
	ca     *CA

	status statusLimit
	sealed sealedTotals
}

// +kubebuilder:rbac:groups=alpheus-dev.org,resources=streamruns,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=alpheus-dev.org,resources=streamruns/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=alpheus-dev.org,resources=streamruns/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;delete;deletecollection;patch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups="",resources=pods/log,verbs=get
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=get;list;watch;create
// +kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies,verbs=get;list;watch;create;update

// SetupWithManager registers the reconciler; it watches the pods,
// ServiceAccounts and NetworkPolicies it owns.
func (rc *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	if rc.Options.Resync == 0 {
		rc.Options.Resync = 2 * time.Second
	}
	if rc.Options.StatusInterval == 0 {
		rc.Options.StatusInterval = time.Second
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.StreamRun{}).
		Owns(&corev1.Pod{}).
		Owns(&corev1.ServiceAccount{}).
		Owns(&networkingv1.NetworkPolicy{}).
		Named("streamrun").
		Complete(rc)
}

// Reconcile brings one run closer to its desired state and patches its
// status when it changed.
func (rc *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var r v1alpha1.StreamRun
	if err := rc.Get(ctx, req.NamespacedName, &r); err != nil {
		if apierrors.IsNotFound(err) {
			rc.status.forget(req.NamespacedName)
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !r.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, rc.finalize(ctx, &r)
	}
	// Every change of a pod starts a pass, so a run with many pods would
	// write its status many times a second. A pass that comes too soon
	// after the last write waits for the rest of the interval (GO-22).
	if wait := rc.status.wait(req.NamespacedName, time.Now(), rc.Options.StatusInterval); wait > 0 {
		return ctrl.Result{RequeueAfter: wait}, nil
	}
	orig := r.DeepCopy()
	if controllerutil.AddFinalizer(&r, Finalizer) {
		// A patch of the finalizers only: an update would write the spec
		// back as Go encodes it, such as 168h as 168h0m0s.
		if err := rc.Patch(ctx, &r, client.MergeFromWithOptions(orig, client.MergeFromWithOptimisticLock{})); err != nil {
			return ctrl.Result{}, err
		}
	}
	if err := rc.holdSecret(ctx, &r); err != nil {
		return ctrl.Result{}, err
	}

	before := r.DeepCopy()
	ev := &eventBuffer{}
	res, err := rc.reconcile(withEvents(ctx, ev), &r)
	if !equality.Semantic.DeepEqual(before.Status, r.Status) {
		r.Status.ObservedGeneration = r.Generation
		if perr := rc.Status().Patch(ctx, &r, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); perr != nil {
			if apierrors.IsConflict(perr) {
				// The events of this pass go with it: the next pass sees
				// the same changes and records them again.
				return ctrl.Result{RequeueAfter: 100 * time.Millisecond}, nil
			}
			return ctrl.Result{}, perr
		}
		rc.status.wrote(req.NamespacedName, time.Now())
	}
	ev.flush(rc.Recorder, &r)
	return res, err
}

func terminal(p v1alpha1.RunPhase) bool { return Terminal(p) }

// Terminal reports whether a run in phase p has finished.
func Terminal(p v1alpha1.RunPhase) bool {
	switch p {
	case v1alpha1.RunSucceeded, v1alpha1.RunPartiallySucceeded, v1alpha1.RunFailed, v1alpha1.RunCancelled:
		return true
	}
	return false
}

func (rc *Reconciler) reconcile(ctx context.Context, r *v1alpha1.StreamRun) (ctrl.Result, error) {
	st := &r.Status
	// A finished run keeps them too: a replay pod uses them (GO-141).
	if err := rc.ensureRunObjects(ctx, r); err != nil {
		var no *errNotOurs
		if !errors.As(err, &no) {
			return ctrl.Result{}, err
		}
		if !terminal(st.Phase) {
			rc.fail(ctx, r, v1alpha1.ReasonInvalidSpec, err.Error())
			return ctrl.Result{}, nil
		}
	}
	if terminal(st.Phase) {
		return rc.afterFinish(ctx, r)
	}
	g, err := newGraph(&r.Spec)
	if err != nil {
		rc.fail(ctx, r, v1alpha1.ReasonInvalidSpec, "invalid graph: "+err.Error())
		return ctrl.Result{}, nil
	}
	if !rc.Options.AllowTags {
		for _, s := range r.Spec.Steps {
			if !Pinned(s.Image) {
				rc.fail(ctx, r, v1alpha1.ReasonInvalidSpec, fmt.Sprintf("step %s: image %s is not pinned by digest (@sha256:...)", s.Name, s.Image))
				return ctrl.Result{}, nil
			}
		}
	}
	if st.Phase == "" {
		st.Phase = v1alpha1.RunPending
		st.StartedAt = ptrNow()
		emit(ctx, corev1.EventTypeNormal, "Started", "Start", "started %d steps", len(r.Spec.Steps))
		afterWrite(ctx, func() { runsStarted.WithLabelValues(r.Namespace).Inc() })
	}
	initStatus(r)

	if r.Spec.Cancel {
		// afterFinish deletes the pods once the phase is written.
		st.Phase, st.Message, st.Reason = v1alpha1.RunCancelled, "cancelled", v1alpha1.ReasonCancelled
		rc.finish(ctx, r, g)
		return ctrl.Result{}, nil
	}
	pods, err := rc.pods(ctx, r)
	if err != nil {
		return ctrl.Result{}, err
	}
	if st.AgentImage == "" {
		st.AgentImage = runAgent(pods, rc.Options.AgentImage)
	}
	var stop *stopped
	for i := range r.Spec.Steps {
		s := &r.Spec.Steps[i]
		ss := &st.Steps[i]
		for j := range ss.Replicas {
			why, err := rc.replica(ctx, r, g, s, &ss.Replicas[j], pods)
			if err != nil {
				return ctrl.Result{}, err
			}
			if why != nil && stop == nil {
				stop = why
			}
		}
		ss.Phase = stepPhase(ss.Replicas)
	}

	// No agent can work with keys the storage refuses, and no restart
	// helps, so the run fails at once (GO-137).
	if err := rc.refreshStorage(ctx, r, g); store.Refused(err) && !store.QuotaExceeded(err) && stop == nil {
		stop = &stopped{reason: v1alpha1.ReasonStorageRefused,
			msg: fmt.Sprintf("the storage refused the keys in Secret %s: %v", r.Spec.Storage.SecretRef, err)}
	}

	finished, failed, running := 0, 0, false
	for _, ss := range st.Steps {
		switch ss.Phase {
		case v1alpha1.StepSucceeded:
			finished++
		case v1alpha1.StepFailed:
			finished++
			failed++
		case v1alpha1.StepRunning:
			running = true
		}
	}
	st.StepsFinished = fmt.Sprintf("%d/%d", finished, len(st.Steps))
	switch {
	case stop != nil:
		// The pods go in afterFinish, once the phase is written: deleted
		// now, a status write that conflicts would leave the last attempt
		// looking deleted instead of failed.
		st.Phase, st.Message, st.Reason = v1alpha1.RunFailed, stop.msg, stop.reason
		if stop.step != "" {
			st.FailedStep, st.FailedReplica = stop.step, ptr.To(stop.replica)
		}
		rc.finish(ctx, r, g)
		return ctrl.Result{}, nil
	case finished == len(st.Steps):
		st.Phase, st.Message, st.Reason = v1alpha1.RunSucceeded, "", ""
		if failed > 0 {
			st.Phase, st.Message, st.Reason = v1alpha1.RunPartiallySucceeded, failedStepsMessage(failed), v1alpha1.ReasonFailedReplicas
		}
		rc.finish(ctx, r, g)
		return ctrl.Result{}, nil
	case running:
		st.Phase = v1alpha1.RunRunning
	}
	setCondition(r, v1alpha1.ConditionProgressing, metav1.ConditionTrue, "Running", "")
	return ctrl.Result{RequeueAfter: rc.Options.Resync}, nil
}

// initStatus gives every step and replica an entry, in spec order.
func initStatus(r *v1alpha1.StreamRun) {
	if len(r.Status.Steps) == len(r.Spec.Steps) {
		return
	}
	r.Status.Steps = make([]v1alpha1.StepStatus, len(r.Spec.Steps))
	for i, s := range r.Spec.Steps {
		ss := &r.Status.Steps[i]
		ss.Name, ss.Phase = s.Name, v1alpha1.StepPending
		for j := range replicas(&s) {
			ss.Replicas = append(ss.Replicas, v1alpha1.ReplicaStatus{Index: int32(j), Phase: v1alpha1.StepPending, Attempt: 1})
		}
	}
}

// replica advances one replica: it creates the pod of the current attempt,
// follows it, and starts a new attempt when it failed and its pod is
// confirmed gone (GO-67, GO-77). It says why when the replica failed for
// good and the run must stop.
func (rc *Reconciler) replica(ctx context.Context, r *v1alpha1.StreamRun, g *graph, s *v1alpha1.Step, rs *v1alpha1.ReplicaStatus, pods map[string]*corev1.Pod) (*stopped, error) {
	if rs.Phase == v1alpha1.StepSucceeded || rs.Phase == v1alpha1.StepFailed {
		return nil, nil
	}
	name := podName(r.Name, s.Name, int(rs.Index), rs.Attempt)
	pod := pods[name]
	if pod == nil {
		if rs.Pod == name {
			// The cache may not have a pod just created yet; only the API
			// server can confirm that it is gone.
			gone, err := rc.podGone(ctx, r.Namespace, name)
			if err != nil || !gone {
				return nil, err
			}
			// The attempt's pod existed and is gone, so it is safe to start
			// another (GO-67). If the controller deleted it for the agent's
			// restarts, that is the reason.
			reason, msg := v1alpha1.ReasonPodDeleted, "the pod was deleted"
			if strings.HasPrefix(rs.Message, "the agent restarted ") {
				reason, msg = rs.Reason, rs.Message
			}
			return rc.failedAttempt(ctx, r, g, s, rs, reason, msg)
		}
		if rc.Options.HubAddress != "" {
			if err := rc.ensureSignalSecret(ctx, r, s.Name, int(rs.Index), rs.Attempt); err != nil {
				return nil, err
			}
		}
		p, err := rc.pod(r, g, s, int(rs.Index), rs.Attempt)
		if err != nil {
			return nil, err
		}
		if err := controllerutil.SetControllerReference(r, p, rc.Scheme); err != nil {
			return nil, err
		}
		if err := rc.Create(ctx, p); err != nil && !apierrors.IsAlreadyExists(err) {
			return nil, err
		}
		rs.Pod, rs.Phase = name, v1alpha1.StepPending
		return nil, nil
	}

	rs.Pod = name
	switch pod.Status.Phase {
	case corev1.PodSucceeded:
		rs.Phase, rs.Message, rs.Reason = v1alpha1.StepSucceeded, "", ""
	case corev1.PodFailed:
		// Every container has stopped, so the attempt is over (GO-67). Its
		// logs are kept before the next attempt replaces it (GO-140).
		reason, msg := failure(pod)
		rc.keepLogs(ctx, r, s, rs, pod)
		return rc.failedAttempt(ctx, r, g, s, rs, reason, msg)
	case corev1.PodRunning:
		rs.Phase = v1alpha1.StepRunning
		if agentRestarts(pod) >= agentRestartLimit && pod.DeletionTimestamp.IsZero() {
			rs.Reason, rs.Message = v1alpha1.ReasonAgentFailed, fmt.Sprintf("the agent restarted %d times", agentRestarts(pod))
			if reason, m := agentFailure(pod); m != "" {
				rs.Reason, rs.Message = reason, rs.Message+": "+m
			}
			rc.keepLogs(ctx, r, s, rs, pod)
			if err := rc.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
				return nil, err
			}
		}
	default:
		// Pending, or Unknown: a pod on a node without contact blocks a new
		// attempt until it is deleted (GO-67).
	}
	return nil, nil
}

// stopped is why a run must stop: a replica that failed for good, or keys
// the storage refuses.
type stopped struct {
	reason  v1alpha1.Reason
	msg     string
	step    string
	replica int32
}

// failedAttempt starts the next attempt, or fails the replica when it has
// used all of its restarts. With failurePolicy continue the replica's
// streams are closed instead of stopping the run (GO-97).
//
// A full bucket stops the run at once, whatever the restarts and the
// failure policy: a new attempt would write to the same full bucket, and
// so would every other replica, also the one closing the streams.
func (rc *Reconciler) failedAttempt(ctx context.Context, r *v1alpha1.StreamRun, g *graph, s *v1alpha1.Step, rs *v1alpha1.ReplicaStatus, reason v1alpha1.Reason, msg string) (*stopped, error) {
	rs.Reason = reason
	rc.captureFailure(ctx, r, g, s, rs, reason, msg)
	if reason == v1alpha1.ReasonStorageFull {
		emit(ctx, corev1.EventTypeWarning, string(reason), "GiveUp", "step %s replica %d attempt %d failed, and a full bucket does not get emptier by a restart: %s",
			s.Name, rs.Index, rs.Attempt, msg)
		rs.Phase, rs.Message = v1alpha1.StepFailed, msg
		return &stopped{reason: reason, step: s.Name, replica: rs.Index,
			msg: fmt.Sprintf("step %s replica %d failed: the bucket is full: %s", s.Name, rs.Index, msg)}, nil
	}
	if rs.Restarts < maxRestarts(s) {
		emit(ctx, corev1.EventTypeWarning, string(reason), "Restart", "step %s replica %d attempt %d failed, restarting (%d of %d): %s",
			s.Name, rs.Index, rs.Attempt, rs.Restarts+1, maxRestarts(s), msg)
		afterWrite(ctx, func() { stepRestarts.WithLabelValues(r.Namespace, s.Name, string(reason)).Inc() })
		rs.Restarts++
		rs.Attempt++
		rs.Pod, rs.Phase, rs.Message = "", v1alpha1.StepPending, fmt.Sprintf("attempt %d failed: %s", rs.Attempt-1, msg)
		return nil, nil
	}
	emit(ctx, corev1.EventTypeWarning, string(reason), "GiveUp", "step %s replica %d attempt %d failed after %d restarts: %s",
		s.Name, rs.Index, rs.Attempt, rs.Restarts, msg)
	rs.Phase, rs.Message = v1alpha1.StepFailed, msg
	if s.FailurePolicy == v1alpha1.FailureContinue {
		failed, err := rc.closeStreams(ctx, r, g, s, int(rs.Index), int64(rs.Attempt)+1)
		if err != nil {
			return nil, err
		}
		rs.FailedChunks = failed
		return nil, nil
	}
	return &stopped{reason: reason, step: s.Name, replica: rs.Index,
		msg: fmt.Sprintf("step %s replica %d failed after %d restarts: %s", s.Name, rs.Index, rs.Restarts, msg)}, nil
}

// failure says why a pod failed (GO-138): from the component's exit and
// what it left in its termination message or log (GO-139), and when the
// agent failed too, the agent's reason, such as storage that is
// unreachable (GO-136). When the agent failed, its reason is the reason:
// the component then only got SIGTERM.
func failure(pod *corev1.Pod) (v1alpha1.Reason, string) {
	reason, msg := v1alpha1.Reason(""), ""
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.Name == ComponentContainer && cs.State.Terminated != nil {
			t := cs.State.Terminated
			reason, msg = v1alpha1.ReasonComponentFailed, fmt.Sprintf("the component exited with code %d", t.ExitCode)
			if m := strings.TrimSpace(t.Message); m != "" {
				msg += ": " + m
			}
		}
	}
	if msg == "" {
		reason, msg = v1alpha1.ReasonPodFailed, pod.Status.Message
	}
	if ar, m := agentFailure(pod); m != "" {
		if msg == "" {
			return ar, "the agent failed: " + m
		}
		return ar, msg + "; the agent failed: " + m
	}
	if msg == "" {
		return v1alpha1.ReasonPodFailed, "the pod failed"
	}
	return reason, msg
}

// agentFailure is the reason and message the agent last failed with, if it
// did. The agent puts its reason in front of the message (GO-138).
func agentFailure(pod *corev1.Pod) (v1alpha1.Reason, string) {
	for _, cs := range pod.Status.InitContainerStatuses {
		if cs.Name != AgentContainer {
			continue
		}
		for _, t := range []*corev1.ContainerStateTerminated{cs.State.Terminated, cs.LastTerminationState.Terminated} {
			if t != nil && t.ExitCode != 0 && t.Message != "" {
				return splitReason(strings.TrimSpace(t.Message))
			}
		}
	}
	return "", ""
}

// splitReason takes the reason off the front of an agent's message, such
// as "StorageUnreachable: …". A message without one is AgentFailed.
func splitReason(msg string) (v1alpha1.Reason, string) {
	if code, rest, ok := strings.Cut(msg, ": "); ok {
		if slices.Contains(v1alpha1.Reasons(), v1alpha1.Reason(code)) {
			return v1alpha1.Reason(code), rest
		}
	}
	return v1alpha1.ReasonAgentFailed, msg
}

func agentRestarts(pod *corev1.Pod) int32 {
	for _, cs := range pod.Status.InitContainerStatuses {
		if cs.Name == AgentContainer {
			return cs.RestartCount
		}
	}
	return 0
}

func stepPhase(rs []v1alpha1.ReplicaStatus) v1alpha1.StepPhase {
	done, failed, running := 0, 0, false
	for _, r := range rs {
		switch r.Phase {
		case v1alpha1.StepSucceeded:
			done++
		case v1alpha1.StepFailed:
			done++
			failed++
		case v1alpha1.StepRunning:
			running = true
		}
	}
	switch {
	case failed > 0 && done == len(rs):
		return v1alpha1.StepFailed
	case done == len(rs):
		return v1alpha1.StepSucceeded
	case running || done > 0:
		return v1alpha1.StepRunning
	}
	return v1alpha1.StepPending
}

// stopUnfinished marks the steps and replicas that had not finished when
// the run ended as Stopped, so the status does not say they still run.
func stopUnfinished(st *v1alpha1.StreamRunStatus) {
	stop := func(p *v1alpha1.StepPhase) {
		if *p == "" || *p == v1alpha1.StepPending || *p == v1alpha1.StepRunning {
			*p = v1alpha1.StepStopped
		}
	}
	for i := range st.Steps {
		stop(&st.Steps[i].Phase)
		for j := range st.Steps[i].Replicas {
			stop(&st.Steps[i].Replicas[j].Phase)
		}
	}
}

// runAgent is the agent a run without status.agentImage uses from now on
// (GO-164): the one its oldest pod runs, so a run that started under an
// older controller keeps its agent, or else the controller's.
func runAgent(pods map[string]*corev1.Pod, controllers string) string {
	var oldest *corev1.Pod
	for _, p := range pods {
		if oldest == nil || p.CreationTimestamp.Before(&oldest.CreationTimestamp) ||
			p.CreationTimestamp.Equal(&oldest.CreationTimestamp) && p.Name < oldest.Name {
			oldest = p
		}
	}
	if oldest != nil {
		for _, c := range oldest.Spec.InitContainers {
			if c.Name == AgentContainer && c.Image != "" {
				return c.Image
			}
		}
	}
	return controllers
}

// pods lists the run's pods by name.
func (rc *Reconciler) pods(ctx context.Context, r *v1alpha1.StreamRun) (map[string]*corev1.Pod, error) {
	var list corev1.PodList
	if err := rc.List(ctx, &list, client.InNamespace(r.Namespace), client.MatchingLabels{LabelRun: r.Name}); err != nil {
		return nil, err
	}
	pods := map[string]*corev1.Pod{}
	for i := range list.Items {
		p := &list.Items[i]
		if metav1.IsControlledBy(p, r) {
			pods[p.Name] = p
		}
	}
	return pods, nil
}

func (rc *Reconciler) deletePods(ctx context.Context, r *v1alpha1.StreamRun) error {
	return client.IgnoreNotFound(rc.DeleteAllOf(ctx, &corev1.Pod{}, client.InNamespace(r.Namespace), client.MatchingLabels{LabelRun: r.Name}))
}

// fail ends a run that cannot start, such as one with an invalid graph.
func (rc *Reconciler) fail(ctx context.Context, r *v1alpha1.StreamRun, reason v1alpha1.Reason, msg string) {
	r.Status.Phase, r.Status.Message, r.Status.Reason = v1alpha1.RunFailed, msg, reason
	if r.Status.StartedAt == nil {
		r.Status.StartedAt = ptrNow()
	}
	rc.finish(ctx, r, nil)
}

// finish sets the end of a run and writes what outlives it: the summary
// (GO-123) and finishedAt in run.json (GO-98).
func (rc *Reconciler) finish(ctx context.Context, r *v1alpha1.StreamRun, g *graph) {
	r.Status.FinishedAt = ptrNow()
	stopUnfinished(&r.Status)
	status := metav1.ConditionTrue
	if r.Status.Phase != v1alpha1.RunSucceeded {
		status = metav1.ConditionFalse
	}
	reason := string(r.Status.Phase)
	if r.Status.Reason != "" {
		reason = string(r.Status.Reason)
	}
	setCondition(r, v1alpha1.ConditionReady, status, reason, r.Status.Message)
	setCondition(r, v1alpha1.ConditionProgressing, metav1.ConditionFalse, string(r.Status.Phase), "")
	if r.Status.Phase == v1alpha1.RunPartiallySucceeded {
		setCondition(r, v1alpha1.ConditionDegraded, metav1.ConditionTrue, string(v1alpha1.ReasonFailedReplicas), r.Status.Message)
	}
	typ, note := corev1.EventTypeNormal, "the run "+phaseWords(r.Status.Phase)
	if r.Status.Phase != v1alpha1.RunSucceeded {
		typ = corev1.EventTypeWarning
	}
	if r.Status.Message != "" {
		note += ": " + r.Status.Message
	}
	emit(ctx, typ, reason, "Finish", "%s", note)
	phase, ns, name := string(r.Status.Phase), r.Namespace, r.Name
	afterWrite(ctx, func() {
		runsFinished.WithLabelValues(ns, phase, string(r.Status.Reason)).Inc()
		edgeLag.DeletePartialMatch(prometheus.Labels{"namespace": ns, "run": name})
	})
	if g != nil {
		// The status keeps the counts last read; the run finishes anyway.
		if err := rc.refreshStorage(ctx, r, g); err != nil {
			log.FromContext(ctx).Error(err, "read the storage at the end of the run")
		}
		rc.keepSkipped(ctx, r, g)
	}
	if err := rc.writeSummary(ctx, r); err != nil {
		log.FromContext(ctx).Error(err, "summary")
	}
}

// afterFinish deletes what a finished run keeps: its pods and runs/<uid>/
// after ttlAfterFinished (SPEC 6.1 step 6), and its results after
// resultsTTL (GO-78).
func (rc *Reconciler) afterFinish(ctx context.Context, r *v1alpha1.StreamRun) (ctrl.Result, error) {
	if r.Status.FinishedAt == nil {
		return ctrl.Result{}, nil
	}
	if r.Status.Phase == v1alpha1.RunFailed || r.Status.Phase == v1alpha1.RunCancelled {
		// Stop what still runs.
		pods, err := rc.pods(ctx, r)
		if err != nil {
			return ctrl.Result{}, err
		}
		if len(pods) > 0 {
			if err := rc.deletePods(ctx, r); err != nil {
				return ctrl.Result{}, err
			}
		}
	}
	now := time.Now()
	var next time.Duration
	if ttl := r.Spec.TTLAfterFinished; ttl != nil {
		if left := r.Status.FinishedAt.Add(ttl.Duration).Sub(now); left > 0 {
			next = left
		} else {
			if err := rc.deletePods(ctx, r); err != nil {
				return ctrl.Result{}, err
			}
			if err := rc.deletePrefix(ctx, r, chunk.RunPrefix(string(r.UID))); err != nil {
				return ctrl.Result{}, err
			}
		}
	}
	if ttl := r.Spec.ResultsTTL; ttl != nil && ttl.Duration > 0 {
		if left := r.Status.FinishedAt.Add(ttl.Duration).Sub(now); left > 0 {
			if next == 0 || left < next {
				next = left
			}
		} else if err := rc.deletePrefix(ctx, r, "results/"+r.Name+"/"+string(r.UID)+"/"); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{RequeueAfter: next}, nil
}

// finalize deletes the run's storage prefix, not its results (GO-20,
// GO-78), and lets the run go.
func (rc *Reconciler) finalize(ctx context.Context, r *v1alpha1.StreamRun) error {
	edgeLag.DeletePartialMatch(prometheus.Labels{"namespace": r.Namespace, "run": r.Name})
	rc.sealed.forget(string(r.UID))
	if !controllerutil.ContainsFinalizer(r, Finalizer) {
		return nil
	}
	var secret corev1.Secret
	err := rc.Get(ctx, client.ObjectKey{Namespace: r.Namespace, Name: r.Spec.Storage.SecretRef}, &secret)
	switch {
	case apierrors.IsNotFound(err):
		// Only for a Secret that never existed or that went before the
		// run held it: without keys, the data cannot be deleted.
		log.FromContext(ctx).Info("the storage Secret is gone; the run's data is left in the bucket",
			"secret", r.Spec.Storage.SecretRef, "prefix", chunk.RunPrefix(string(r.UID)))
	case err != nil:
		return err
	default:
		err := rc.deletePrefix(ctx, r, chunk.RunPrefix(string(r.UID)))
		switch {
		case store.Refused(err):
			// Keys the storage refuses can never delete the data, so
			// waiting would only keep the run, and its namespace, from
			// going (GO-137).
			log.FromContext(ctx).Info("the storage refuses the keys; the run's data is left in the bucket",
				"secret", r.Spec.Storage.SecretRef, "prefix", chunk.RunPrefix(string(r.UID)), "error", err.Error())
		case err != nil:
			return err
		}
		before := secret.DeepCopy()
		if controllerutil.RemoveFinalizer(&secret, secretFinalizer(r)) {
			if err := rc.Patch(ctx, &secret, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
				return err
			}
		}
	}
	before := r.DeepCopy()
	controllerutil.RemoveFinalizer(r, Finalizer)
	return rc.Patch(ctx, r, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
}

// podGone asks the API server whether a pod no longer exists.
func (rc *Reconciler) podGone(ctx context.Context, namespace, name string) (bool, error) {
	reader := rc.APIReader
	if reader == nil {
		reader = rc.Client
	}
	err := reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &corev1.Pod{})
	if apierrors.IsNotFound(err) {
		return true, nil
	}
	return false, err
}

// holdSecret puts the run's finalizer on its storage Secret. A missing
// Secret is not an error here; the run fails when it needs the keys.
func (rc *Reconciler) holdSecret(ctx context.Context, r *v1alpha1.StreamRun) error {
	var secret corev1.Secret
	err := rc.Get(ctx, client.ObjectKey{Namespace: r.Namespace, Name: r.Spec.Storage.SecretRef}, &secret)
	if apierrors.IsNotFound(err) || err == nil && !secret.DeletionTimestamp.IsZero() {
		return nil
	}
	if err != nil {
		return err
	}
	before := secret.DeepCopy()
	if !controllerutil.AddFinalizer(&secret, secretFinalizer(r)) {
		return nil
	}
	return rc.Patch(ctx, &secret, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
}

func (rc *Reconciler) deletePrefix(ctx context.Context, r *v1alpha1.StreamRun, prefix string) error {
	st, err := rc.store(ctx, r)
	if err != nil {
		return err
	}
	if err := store.DeletePrefix(ctx, st, prefix); err != nil {
		return fmt.Errorf("delete %s: %w", prefix, err)
	}
	return nil
}

// store opens the run's storage with the keys in its Secret.
func (rc *Reconciler) store(ctx context.Context, r *v1alpha1.StreamRun) (store.Store, error) {
	if rc.Options.NewStore != nil {
		return rc.Options.NewStore(ctx, r)
	}
	return OpenStore(ctx, rc.Client, r, store.Address{CAFile: rc.Options.StorageCAFile})
}

// OpenStore opens a run's storage with the keys in its storage Secret,
// with results/ in the results bucket when the run has one (GO-129).
// at says where the storage is reached from, when that is not
// spec.storage.endpoint, and which CAs to trust.
func OpenStore(ctx context.Context, c client.Reader, r *v1alpha1.StreamRun, at store.Address) (store.Store, error) {
	var secret corev1.Secret
	if err := c.Get(ctx, client.ObjectKey{Namespace: r.Namespace, Name: r.Spec.Storage.SecretRef}, &secret); err != nil {
		return nil, fmt.Errorf("storage secret: %w", err)
	}
	cfg := store.S3Config{
		Endpoint:  r.Spec.Storage.Endpoint,
		Bucket:    r.Spec.Storage.Bucket,
		Region:    r.Spec.Storage.Region,
		AccessKey: string(secret.Data["AWS_ACCESS_KEY_ID"]),
		SecretKey: string(secret.Data["AWS_SECRET_ACCESS_KEY"]),
		CAFile:    at.CAFile,
		CAPEM:     at.CAPEM,
	}
	if at.Endpoint != "" {
		cfg.Endpoint = at.Endpoint
	}
	main, err := store.NewS3(cfg)
	if err != nil || r.Spec.Storage.ResultsBucket == "" {
		return main, err
	}
	cfg.Bucket = r.Spec.Storage.ResultsBucket
	results, err := store.NewS3(cfg)
	if err != nil {
		return nil, err
	}
	return store.WithResults(main, results), nil
}

// ensureSignalSecret gives one attempt of a replica its client certificate
// for the signal hub, naming the attempt, so the hub can turn away older
// attempts (GO-41).
func (rc *Reconciler) ensureSignalSecret(ctx context.Context, r *v1alpha1.StreamRun, step string, replica int, attempt int32) error {
	name := signalSecret(podName(r.Name, step, replica, attempt))
	var s corev1.Secret
	err := rc.Get(ctx, client.ObjectKey{Namespace: r.Namespace, Name: name}, &s)
	if err == nil || !apierrors.IsNotFound(err) {
		return err
	}
	ca, err := rc.loadCA(ctx)
	if err != nil {
		return err
	}
	certPEM, keyPEM, err := ca.IssueAgent(AgentIdentity{
		Namespace: r.Namespace, Run: r.Name, UID: string(r.UID), Step: step, Replica: replica, Attempt: attempt,
	})
	if err != nil {
		return err
	}
	s = corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: r.Namespace, Name: name},
		Type:       corev1.SecretTypeTLS,
		Data: map[string][]byte{
			"ca.crt":                ca.CertPEM(),
			corev1.TLSCertKey:       certPEM,
			corev1.TLSPrivateKeyKey: keyPEM,
		},
	}
	if err := controllerutil.SetControllerReference(r, &s, rc.Scheme); err != nil {
		return err
	}
	return client.IgnoreAlreadyExists(rc.Create(ctx, &s))
}

func (rc *Reconciler) loadCA(ctx context.Context) (*CA, error) {
	rc.caOnce.Lock()
	defer rc.caOnce.Unlock()
	if rc.ca == nil {
		ca, err := LoadCA(ctx, rc.Client, rc.Options.Namespace)
		if err != nil {
			return nil, err
		}
		rc.ca = ca
	}
	return rc.ca, nil
}

func setCondition(r *v1alpha1.StreamRun, typ string, status metav1.ConditionStatus, reason, msg string) {
	if reason == "" {
		reason = "Unknown"
	}
	meta.SetStatusCondition(&r.Status.Conditions, metav1.Condition{
		Type:               typ,
		Status:             status,
		Reason:             reason,
		Message:            msg,
		ObservedGeneration: r.Generation,
	})
}

func ptrNow() *metav1.Time {
	t := metav1.Now()
	return &t
}

// failedStepsMessage is a PartiallySucceeded run's message.
func failedStepsMessage(failed int) string {
	if failed == 1 {
		return "1 step had failed replicas"
	}
	return fmt.Sprintf("%d steps had failed replicas", failed)
}

// phaseWords is a run's phase as words for an event: "partially
// succeeded" rather than "partiallysucceeded".
func phaseWords(p v1alpha1.RunPhase) string {
	if p == v1alpha1.RunPartiallySucceeded {
		return "partially succeeded"
	}
	return strings.ToLower(string(p))
}
