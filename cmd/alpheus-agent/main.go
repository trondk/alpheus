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

// Command alpheus-agent is the sidecar next to each component (SPEC 7). The
// controller starts it as a native sidecar and configures it with
// environment variables:
//
//	ALPHEUS_AGENT_CONFIG   the agent's config as JSON (internal/agent.Config)
//	ALPHEUS_S3_ENDPOINT    storage: endpoint, bucket, region and keys
//	ALPHEUS_S3_BUCKET
//	ALPHEUS_S3_REGION
//	ALPHEUS_S3_ACCESS_KEY
//	ALPHEUS_S3_SECRET_KEY
//	ALPHEUS_S3_CA_FILE     optional CA bundle for the endpoint
//	ALPHEUS_S3_RESULTS_BUCKET  optional bucket for results/ (GO-129)
//	ALPHEUS_S3_TIMEOUT     how long to keep trying while the storage is
//	                       unreachable, as a Go duration; empty means 0 (GO-136)
//	ALPHEUS_METRICS_LISTEN where /metrics, /healthz and /readyz are served,
//	                       such as :9090; empty serves none (GO-51, GO-52)
//	ALPHEUS_SIGNAL_HUB     the builtin signal hub; empty means poll only
//	ALPHEUS_SIGNAL_CA      the hub's CA, and this agent's certificate and
//	ALPHEUS_SIGNAL_CERT    key, issued by the controller per run (GO-41)
//	ALPHEUS_SIGNAL_KEY
//
// After the component has finished, the agent keeps running until it gets
// SIGTERM, as a native sidecar must; -exit-on-finish makes it exit at once,
// for runs outside Kubernetes.
//
// In a replay pod, -fetch-failure <prefix> fetches a kept failed attempt
// into -dest, each chunk checked, and exits (GO-141).
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc/credentials"

	"alpheus-dev.org/alpheus/internal/agent"
	"alpheus-dev.org/alpheus/internal/replay"
	alsignal "alpheus-dev.org/alpheus/internal/signal"
	"alpheus-dev.org/alpheus/internal/signal/builtin"
	"alpheus-dev.org/alpheus/internal/store"
)

func main() {
	exitOnFinish := flag.Bool("exit-on-finish", false, "exit when the component has finished, instead of waiting for SIGTERM")
	fetchFailure := flag.String("fetch-failure", "", "fetch the failed attempt kept at this prefix into -dest and exit, for a replay pod (GO-141)")
	dest := flag.String("dest", "/replay", "with -fetch-failure: the folder to fetch into")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("component", "alpheus-agent")
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	if *fetchFailure != "" {
		if err := fetch(ctx, log, *fetchFailure, *dest); err != nil {
			log.Error("fetch failed", "error", err.Error())
			terminationMessage(err.Error())
			os.Exit(1)
		}
		return
	}
	if err := run(ctx, log, *exitOnFinish); err != nil {
		if ctx.Err() != nil && errors.Is(err, context.Canceled) {
			// Stopped, as Kubernetes stops a sidecar once the component
			// has exited: not a failure, so no reason to report that
			// would hide the component's (GO-138).
			log.Info("agent stopped")
			os.Exit(143)
		}
		log.Error("agent failed", "error", err.Error())
		terminationMessage(string(agent.Reason(err)) + ": " + err.Error())
		os.Exit(1)
	}
}

// terminationMessage leaves why the agent failed where Kubernetes shows it
// in the pod's status, and the controller in the step's (GO-136), with the
// reason code in front (GO-138). The file exists only in a container.
func terminationMessage(msg string) {
	const path = "/dev/termination-log"
	if _, err := os.Stat(path); err != nil {
		return
	}
	if len(msg) > 1024 {
		msg = msg[:1024]
	}
	_ = os.WriteFile(path, []byte(msg), 0o644)
}

func run(ctx context.Context, log *slog.Logger, exitOnFinish bool) error {
	cfg, err := agent.ParseConfig([]byte(os.Getenv("ALPHEUS_AGENT_CONFIG")))
	if err != nil {
		return err
	}
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	metrics := agent.NewMetrics(reg, cfg)
	var running atomic.Pointer[agent.Agent]
	ready := func() bool { a := running.Load(); return a != nil && a.Ready() }
	if addr := os.Getenv("ALPHEUS_METRICS_LISTEN"); addr != "" {
		stopMetrics, err := serveMetrics(addr, reg, ready, log)
		if err != nil {
			return err
		}
		defer stopMetrics()
	}

	st, err := storeFromEnv(log, metrics.Retried)
	if err != nil {
		return err
	}
	go checkStorage(ctx, log)

	var sig alsignal.Driver
	if hub := os.Getenv("ALPHEUS_SIGNAL_HUB"); hub != "" {
		creds, err := hubCredentials()
		if err != nil {
			return err
		}
		c, err := builtin.Dial(hub, creds, log)
		if err != nil {
			return fmt.Errorf("signal hub: %w", err)
		}
		defer c.Close()
		sig = c
	} else {
		log.Info("no signal hub; polling only")
	}

	a, err := agent.New(cfg, st, sig, log)
	if err != nil {
		return err
	}
	a.SetMetrics(metrics)
	running.Store(a)
	if err := a.Run(ctx); err != nil {
		return err
	}
	if !exitOnFinish {
		<-ctx.Done()
	}
	return nil
}

// storeFromEnv opens the storage the ALPHEUS_S3_* variables name.
func storeFromEnv(log *slog.Logger, retried func()) (store.Store, error) {
	s3 := store.S3Config{
		Endpoint:  os.Getenv("ALPHEUS_S3_ENDPOINT"),
		Bucket:    os.Getenv("ALPHEUS_S3_BUCKET"),
		Region:    os.Getenv("ALPHEUS_S3_REGION"),
		AccessKey: os.Getenv("ALPHEUS_S3_ACCESS_KEY"),
		SecretKey: os.Getenv("ALPHEUS_S3_SECRET_KEY"),
		CAFile:    os.Getenv("ALPHEUS_S3_CA_FILE"),
		OnRetry: func(err error, wait time.Duration) {
			log.Warn("storage unreachable; trying again", "error", err.Error(), "retry_in", wait.String())
			retried()
		},
	}
	if t := os.Getenv("ALPHEUS_S3_TIMEOUT"); t != "" {
		var err error
		if s3.Timeout, err = time.ParseDuration(t); err != nil {
			return nil, fmt.Errorf("ALPHEUS_S3_TIMEOUT: %w", err)
		}
	}
	st, err := store.NewS3(s3)
	if err != nil {
		return nil, err
	}
	if b := os.Getenv("ALPHEUS_S3_RESULTS_BUCKET"); b != "" {
		s3.Bucket = b
		results, err := store.NewS3(s3)
		if err != nil {
			return nil, err
		}
		return store.WithResults(st, results), nil
	}
	return st, nil
}

// checkStorage warns when the storage is not one Alpheus is tested with,
// since Alpheus needs strong read-after-write consistency and has only
// seen it in those (GO-69). It only warns: the run goes on.
func checkStorage(ctx context.Context, log *slog.Logger) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	endpoint := os.Getenv("ALPHEUS_S3_ENDPOINT")
	name, err := store.Identify(ctx, store.S3Config{Endpoint: endpoint, CAFile: os.Getenv("ALPHEUS_S3_CA_FILE")})
	switch {
	case err != nil:
		log.Debug("could not tell which storage this is", "endpoint", endpoint, "error", err.Error())
	case name == "":
		log.Warn("the storage is not one Alpheus is tested with ("+strings.Join(store.Tested, ", ")+
			"); it must have strong read-after-write consistency for new objects, overwrites and listings", "endpoint", endpoint)
	default:
		log.Debug("storage", "endpoint", endpoint, "store", name)
	}
}

// fetch fetches a kept failure into dest, each chunk checked against its
// SHA-256, for a replay pod's component (GO-141).
func fetch(ctx context.Context, log *slog.Logger, prefix, dest string) error {
	st, err := storeFromEnv(log, func() {})
	if err != nil {
		return err
	}
	f, data, err := replay.Read(ctx, st, prefix)
	if err != nil {
		return err
	}
	if err := replay.Fetch(ctx, st, prefix, f, dest); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dest, "failure.json"), data, 0o644); err != nil {
		return err
	}
	log.Info("fetched the failed attempt", "step", f.Step, "replica", f.Replica, "attempt", f.Attempt, "inputs", len(f.Inputs), "reason", f.Reason)
	return nil
}

// serveMetrics serves reg on addr's /metrics (GO-51), and /healthz and
// /readyz (GO-52), until the returned function is called.
func serveMetrics(addr string, reg *prometheus.Registry, ready func() bool, log *slog.Logger) (func(), error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("metrics: %w", err)
	}
	srv := &http.Server{Handler: handler(reg, ready), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Warn("metrics server stopped", "error", err.Error())
		}
	}()
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}, nil
}

// handler serves /metrics, /healthz, which answers while the agent's
// process runs, and /readyz, which answers 200 once ready says so and 503
// before.
func handler(reg *prometheus.Registry, ready func() bool) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !ready() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintln(w, "ok")
	})
	return mux
}

// hubCredentials is mTLS with the certificate the controller issued for the
// run (GO-41).
func hubCredentials() (credentials.TransportCredentials, error) {
	caFile, certFile, keyFile := os.Getenv("ALPHEUS_SIGNAL_CA"), os.Getenv("ALPHEUS_SIGNAL_CERT"), os.Getenv("ALPHEUS_SIGNAL_KEY")
	if caFile == "" || certFile == "" || keyFile == "" {
		return nil, errors.New("signal hub: ALPHEUS_SIGNAL_CA, ALPHEUS_SIGNAL_CERT and ALPHEUS_SIGNAL_KEY are required")
	}
	ca, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("signal hub CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, fmt.Errorf("signal hub CA: no certificates in %s", caFile)
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("signal hub certificate: %w", err)
	}
	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		MinVersion:   tls.VersionTLS13,
	}), nil
}
