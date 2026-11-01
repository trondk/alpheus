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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"alpheus-dev.org/alpheus/api/v1alpha1"
	"alpheus-dev.org/alpheus/internal/agent"
)

// Labels on the pods of a run.
const (
	LabelRun     = "alpheus-dev.org/run"
	LabelStep    = "alpheus-dev.org/step"
	LabelReplica = "alpheus-dev.org/replica"
	LabelAttempt = "alpheus-dev.org/attempt"
)

// Container names, and where the shared volume is mounted.
const (
	ComponentContainer = "component"
	AgentContainer     = "agent"
	sharedVolume       = "alpheus"
	sharedPath         = "/run/alpheus"
	socketPath         = sharedPath + "/agent.sock"
	signalVolume       = "signal"
	signalPath         = "/etc/alpheus/signal"
	runUID             = 65532
)

// podName is unique per run, step, replica and attempt, and at most 63
// characters: a run name that would make it longer is cut and gets a hash.
func podName(run, step string, replica int, attempt int32) string {
	suffix := fmt.Sprintf("-%s-%d-%d", step, replica, attempt)
	if len(run)+len(suffix) <= 63 {
		return run + suffix
	}
	h := sha256.Sum256([]byte(run))
	keep := 63 - len(suffix) - 9
	return run[:keep] + "-" + hex.EncodeToString(h[:4]) + suffix
}

// signalSecret is the Secret with a pod's certificate for the signal hub;
// pod names are unique per attempt (GO-41).
func signalSecret(pod string) string { return pod + "-signal" }

// agentConfig is what the agent of one replica needs (GO-128).
func agentConfig(r *v1alpha1.StreamRun, g *graph, s *v1alpha1.Step, replica int, attempt int32) agent.Config {
	cfg := agent.Config{
		Run:        r.Name,
		Namespace:  r.Namespace,
		UID:        string(r.UID),
		Step:       s.Name,
		Replica:    replica,
		Replicas:   replicas(s),
		Attempt:    int64(attempt),
		MaxUnacked: int(r.Spec.Chunks.MaxUnacked),
		InputOrder: string(s.InputOrder),
		Socket:     socketPath,
		Scratch:    sharedPath + "/scratch",
	}
	if r.Spec.ResultsTTL != nil {
		cfg.ResultsTTL = r.Spec.ResultsTTL.Duration.String()
	}
	if q := r.Spec.Chunks.MaxBytes; q != nil {
		cfg.MaxBytes = q.Value()
	}
	if s.MaxOpen != nil {
		cfg.MaxOpen = int(*s.MaxOpen)
	}
	if s.Prefetch != nil {
		cfg.Prefetch = int(*s.Prefetch)
	}
	if s.Scratch != nil {
		cfg.ScratchLimit = s.Scratch.Value()
	}
	for _, e := range g.in[s.Name] {
		cfg.Inputs = append(cfg.Inputs, agent.InputEdge{Edge: edgeName(e), From: e.From, Replicas: replicas(g.steps[e.From]), Port: e.Port})
	}
	for _, e := range g.out[s.Name] {
		cfg.Outputs = append(cfg.Outputs, agent.OutputEdge{Edge: edgeName(e), Output: e.Output, To: e.To, Replicas: replicas(g.steps[e.To])})
	}
	return cfg
}

// pod builds the pod of one attempt of one replica (SPEC 6.2): the
// component as the only regular container and the agent as a native
// sidecar, sharing an emptyDir for the socket and the scratch folder. Only
// the agent gets storage and signal credentials.
func (rc *Reconciler) pod(r *v1alpha1.StreamRun, g *graph, s *v1alpha1.Step, replica int, attempt int32) (*corev1.Pod, error) {
	cfg, err := json.Marshal(agentConfig(r, g, s, replica, attempt))
	if err != nil {
		return nil, err
	}
	settings := ""
	if s.Settings != nil {
		settings = string(s.Settings.Raw)
	}

	agentEnv := []corev1.EnvVar{
		{Name: "ALPHEUS_AGENT_CONFIG", Value: string(cfg)},
		{Name: "ALPHEUS_S3_ENDPOINT", Value: r.Spec.Storage.Endpoint},
		{Name: "ALPHEUS_S3_BUCKET", Value: r.Spec.Storage.Bucket},
		{Name: "ALPHEUS_S3_REGION", Value: r.Spec.Storage.Region},
		{Name: "ALPHEUS_S3_TIMEOUT", Value: storageTimeout(r).String()},
		secretEnv("ALPHEUS_S3_ACCESS_KEY", r.Spec.Storage.SecretRef, "AWS_ACCESS_KEY_ID"),
		secretEnv("ALPHEUS_S3_SECRET_KEY", r.Spec.Storage.SecretRef, "AWS_SECRET_ACCESS_KEY"),
	}
	if b := r.Spec.Storage.ResultsBucket; b != "" {
		agentEnv = append(agentEnv, corev1.EnvVar{Name: "ALPHEUS_S3_RESULTS_BUCKET", Value: b})
	}
	volumes := []corev1.Volume{{
		Name:         sharedVolume,
		VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: s.Scratch}},
	}}
	agentMounts := []corev1.VolumeMount{{Name: sharedVolume, MountPath: sharedPath}}
	if rc.Options.HubAddress != "" {
		agentEnv = append(agentEnv,
			corev1.EnvVar{Name: "ALPHEUS_SIGNAL_HUB", Value: rc.Options.HubAddress},
			corev1.EnvVar{Name: "ALPHEUS_SIGNAL_CA", Value: signalPath + "/ca.crt"},
			corev1.EnvVar{Name: "ALPHEUS_SIGNAL_CERT", Value: signalPath + "/tls.crt"},
			corev1.EnvVar{Name: "ALPHEUS_SIGNAL_KEY", Value: signalPath + "/tls.key"},
		)
		volumes = append(volumes, corev1.Volume{
			Name:         signalVolume,
			VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: signalSecret(podName(r.Name, s.Name, replica, attempt))}},
		})
		agentMounts = append(agentMounts, corev1.VolumeMount{Name: signalVolume, MountPath: signalPath, ReadOnly: true})
	}

	component := corev1.Container{
		Name:    ComponentContainer,
		Image:   s.Image,
		EnvFrom: s.EnvFrom,
		Env: []corev1.EnvVar{
			{Name: "ALPHEUS_SOCKET", Value: socketPath},
			{Name: "ALPHEUS_SETTINGS", Value: settings},
			{Name: "ALPHEUS_REPLICA", Value: strconv.Itoa(replica)},
			{Name: "ALPHEUS_REPLICAS", Value: strconv.Itoa(replicas(s))},
		},
		VolumeMounts:    []corev1.VolumeMount{{Name: sharedVolume, MountPath: sharedPath}},
		SecurityContext: containerSecurity(),
	}
	if s.Resources != nil {
		component.Resources = *s.Resources
	}

	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName(r.Name, s.Name, replica, attempt),
			Namespace: r.Namespace,
			Labels: map[string]string{
				LabelRun:     r.Name,
				LabelStep:    s.Name,
				LabelReplica: strconv.Itoa(replica),
				LabelAttempt: strconv.Itoa(int(attempt)),
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy:                corev1.RestartPolicyNever,
			AutomountServiceAccountToken: ptr.To(false),
			EnableServiceLinks:           ptr.To(false),
			SecurityContext: &corev1.PodSecurityContext{
				RunAsNonRoot:   ptr.To(true),
				RunAsUser:      ptr.To[int64](runUID),
				RunAsGroup:     ptr.To[int64](runUID),
				FSGroup:        ptr.To[int64](runUID),
				SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
			},
			InitContainers: []corev1.Container{{
				Name:            AgentContainer,
				Image:           rc.Options.AgentImage,
				RestartPolicy:   ptr.To(corev1.ContainerRestartPolicyAlways),
				Env:             agentEnv,
				VolumeMounts:    agentMounts,
				SecurityContext: containerSecurity(),
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("50m"),
						corev1.ResourceMemory: resource.MustParse("64Mi"),
					},
				},
			}},
			Containers: []corev1.Container{component},
			Volumes:    volumes,
		},
	}, nil
}

func secretEnv(name, secret, key string) corev1.EnvVar {
	return corev1.EnvVar{Name: name, ValueFrom: &corev1.EnvVarSource{
		SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: secret}, Key: key},
	}}
}

// containerSecurity meets Pod Security level restricted (GO-25).
func containerSecurity() *corev1.SecurityContext {
	return &corev1.SecurityContext{
		AllowPrivilegeEscalation: ptr.To(false),
		ReadOnlyRootFilesystem:   ptr.To(true),
		RunAsNonRoot:             ptr.To(true),
		Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
		SeccompProfile:           &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
	}
}

// Pinned reports whether an image is pinned by digest (GO-26).
func Pinned(image string) bool { return strings.Contains(image, "@sha256:") }
