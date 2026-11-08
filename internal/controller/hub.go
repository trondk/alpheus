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
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"alpheus-dev.org/alpheus/api/v1alpha1"
	"alpheus-dev.org/alpheus/internal/signal"
	"alpheus-dev.org/alpheus/internal/signal/builtin"
)

// LabelHub marks the controller pod that runs the signal hub; the hub's
// Service selects it (GO-73).
const LabelHub = "alpheus-dev.org/signal-hub"

// Hub runs the builtin signal hub in the leader only (GO-38, GO-73), with
// mTLS from the signal CA (GO-41).
type Hub struct {
	Client client.Client
	// Addr is where the hub listens, such as ":7070".
	Addr string
	// DNSNames are the names agents reach the hub by, for its
	// certificate.
	DNSNames []string
	// Namespace and PodName are the controller's own pod, labelled while
	// it runs the hub. An empty PodName labels nothing, as outside a
	// cluster.
	Namespace, PodName string
}

// NeedLeaderElection makes the manager start the hub only in the leader.
func (h *Hub) NeedLeaderElection() bool { return true }

// Start serves the hub until ctx ends.
func (h *Hub) Start(ctx context.Context) error {
	logger := log.FromContext(ctx).WithName("signal-hub")
	ca, err := LoadCA(ctx, h.Client, h.Namespace)
	if err != nil {
		return err
	}
	certPEM, keyPEM, err := ca.Issue("alpheus-signal-hub", h.DNSNames...)
	if err != nil {
		return err
	}
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return err
	}
	creds := credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    ca.Pool(),
		MinVersion:   tls.VersionTLS13,
	})
	lis, err := net.Listen("tcp", h.Addr)
	if err != nil {
		return fmt.Errorf("signal hub: %w", err)
	}
	srv := grpc.NewServer(grpc.Creds(creds))
	builtin.NewHub(h.authorize).Register(srv)
	go func() {
		if err := srv.Serve(lis); err != nil {
			logger.Error(err, "signal hub stopped; agents poll instead")
		}
	}()
	logger.Info("signal hub started", "addr", h.Addr)

	if err := h.label(context.WithoutCancel(ctx), true); err != nil {
		srv.Stop()
		return err
	}
	<-ctx.Done()
	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := h.label(stopCtx, false); err != nil {
		logger.Error(err, "remove the hub label")
	}
	done := make(chan struct{})
	go func() { srv.GracefulStop(); close(done) }()
	select {
	case <-done:
	case <-stopCtx.Done():
		srv.Stop()
	}
	return nil
}

// authorize lets an agent use only its own run, and only while its attempt
// is the replica's current one, so an agent of an older attempt that
// somehow still runs is turned away (GO-41). Topics are per namespace.
func (h *Hub) authorize(ctx context.Context, run string) (string, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return "", errors.New("no peer")
	}
	info, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok || len(info.State.PeerCertificates) == 0 {
		return "", errors.New("no client certificate")
	}
	id, err := ParseAgentIdentity(info.State.PeerCertificates[0])
	if err != nil {
		return "", err
	}
	if want := signal.RunKey(id.Run, id.UID); run != want {
		return "", fmt.Errorf("the certificate is for run %s, not %s", want, run)
	}
	var r v1alpha1.StreamRun
	if err := h.Client.Get(ctx, client.ObjectKey{Namespace: id.Namespace, Name: id.Run}, &r); err != nil {
		return "", fmt.Errorf("run %s/%s: %w", id.Namespace, id.Run, err)
	}
	if string(r.UID) != id.UID {
		return "", fmt.Errorf("the certificate is for an earlier run named %s/%s", id.Namespace, id.Run)
	}
	for _, s := range r.Status.Steps {
		if s.Name != id.Step {
			continue
		}
		for _, rs := range s.Replicas {
			if int(rs.Index) == id.Replica && id.Attempt < rs.Attempt {
				return "", fmt.Errorf("attempt %d of %s/%d is older than the current attempt %d", id.Attempt, id.Step, id.Replica, rs.Attempt)
			}
		}
	}
	return id.Namespace, nil
}

// label sets or removes LabelHub on the controller's own pod.
func (h *Hub) label(ctx context.Context, on bool) error {
	if h.PodName == "" {
		return nil
	}
	var pod corev1.Pod
	if err := h.Client.Get(ctx, client.ObjectKey{Namespace: h.Namespace, Name: h.PodName}, &pod); err != nil {
		return fmt.Errorf("signal hub label: %w", err)
	}
	before := pod.DeepCopy()
	if on {
		if pod.Labels == nil {
			pod.Labels = map[string]string{}
		}
		pod.Labels[LabelHub] = "active"
	} else {
		delete(pod.Labels, LabelHub)
	}
	return h.Client.Patch(ctx, &pod, client.MergeFrom(before))
}
