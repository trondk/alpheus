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

// Command alpheus-controller reconciles StreamRuns (SPEC 6) and, in the
// leader, runs the builtin signal hub (GO-38, GO-73). Its own pod's name
// and namespace come from the environment variables POD_NAME and
// POD_NAMESPACE (the downward API).
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"alpheus-dev.org/alpheus/api/v1alpha1"
	"alpheus-dev.org/alpheus/internal/controller"
)

func main() {
	var (
		agentImage  = flag.String("agent-image", "", "the agent's image, pinned by digest (GO-24)")
		allowTags   = flag.Bool("allow-tags", false, "allow component images without a digest, for development (GO-26)")
		hubAddr     = flag.String("hub-listen", ":7070", "where the signal hub listens; empty disables it")
		hubAddress  = flag.String("hub-address", "", "where agents reach the hub, such as alpheus-signal-hub.alpheus-system.svc:7070; empty means agents only poll")
		storageCA   = flag.String("storage-ca-file", "", "CA bundle for storage endpoints")
		metricsAddr = flag.String("metrics-listen", ":9090", "where /metrics is served (GO-51)")
		probeAddr   = flag.String("health-listen", ":8081", "where /healthz and /readyz are served (GO-52)")
	)
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("component", "alpheus-controller")
	ctrl.SetLogger(logr.FromSlogHandler(log.Handler()))
	if err := run(*agentImage, *allowTags, *hubAddr, *hubAddress, *storageCA, *metricsAddr, *probeAddr); err != nil {
		log.Error("controller failed", "error", err.Error())
		os.Exit(1)
	}
}

func run(agentImage string, allowTags bool, hubAddr, hubAddress, storageCA, metricsAddr, probeAddr string) error {
	if !strings.Contains(agentImage, "@sha256:") && !allowTags {
		return fmt.Errorf("-agent-image %q must be pinned by digest (GO-24)", agentImage)
	}
	namespace := os.Getenv("POD_NAMESPACE")
	if namespace == "" {
		namespace = "alpheus-system"
	}

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return err
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		return err
	}
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                  scheme,
		Metrics:                 metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress:  probeAddr,
		LeaderElection:          true,
		LeaderElectionID:        "alpheus-controller",
		LeaderElectionNamespace: namespace,
	})
	if err != nil {
		return err
	}
	kube, err := kubernetes.NewForConfig(mgr.GetConfig())
	if err != nil {
		return err
	}
	rc := &controller.Reconciler{
		Client:    mgr.GetClient(),
		APIReader: mgr.GetAPIReader(),
		Scheme:    scheme,
		Recorder:  mgr.GetEventRecorder("alpheus-controller"),
		Logs: func(ctx context.Context, namespace, pod, container string, previous bool) (io.ReadCloser, error) {
			return kube.CoreV1().Pods(namespace).GetLogs(pod, &corev1.PodLogOptions{Container: container, Previous: previous}).Stream(ctx)
		},
		Options: controller.Options{
			AgentImage:    agentImage,
			AllowTags:     allowTags,
			HubAddress:    hubAddress,
			Namespace:     namespace,
			StorageCAFile: storageCA,
		},
	}
	if err := rc.SetupWithManager(mgr); err != nil {
		return err
	}
	if hubAddr != "" {
		host, _, _ := strings.Cut(hubAddress, ":")
		if err := mgr.Add(&controller.Hub{
			Client:    mgr.GetClient(),
			Addr:      hubAddr,
			DNSNames:  []string{host},
			Namespace: namespace,
			PodName:   os.Getenv("POD_NAME"),
		}); err != nil {
			return err
		}
	}
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return err
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return err
	}
	return mgr.Start(ctrl.SetupSignalHandler())
}
