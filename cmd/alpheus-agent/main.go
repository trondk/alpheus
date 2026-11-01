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
//	ALPHEUS_SIGNAL_HUB     the builtin signal hub; empty means poll only
//	ALPHEUS_SIGNAL_CA      the hub's CA, and this agent's certificate and
//	ALPHEUS_SIGNAL_CERT    key, issued by the controller per run (GO-41)
//	ALPHEUS_SIGNAL_KEY
//
// After the component has finished, the agent keeps running until it gets
// SIGTERM, as a native sidecar must; -exit-on-finish makes it exit at once,
// for runs outside Kubernetes.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc/credentials"

	"alpheus-dev.org/alpheus/internal/agent"
	alsignal "alpheus-dev.org/alpheus/internal/signal"
	"alpheus-dev.org/alpheus/internal/signal/builtin"
	"alpheus-dev.org/alpheus/internal/store"
)

func main() {
	exitOnFinish := flag.Bool("exit-on-finish", false, "exit when the component has finished, instead of waiting for SIGTERM")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("component", "alpheus-agent")
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	if err := run(ctx, log, *exitOnFinish); err != nil {
		log.Error("agent failed", "error", err.Error())
		terminationMessage(err.Error())
		os.Exit(1)
	}
}

// terminationMessage leaves why the agent failed where Kubernetes shows it
// in the pod's status, and the controller in the step's (GO-136). The file
// exists only in a container.
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
	s3 := store.S3Config{
		Endpoint:  os.Getenv("ALPHEUS_S3_ENDPOINT"),
		Bucket:    os.Getenv("ALPHEUS_S3_BUCKET"),
		Region:    os.Getenv("ALPHEUS_S3_REGION"),
		AccessKey: os.Getenv("ALPHEUS_S3_ACCESS_KEY"),
		SecretKey: os.Getenv("ALPHEUS_S3_SECRET_KEY"),
		CAFile:    os.Getenv("ALPHEUS_S3_CA_FILE"),
		OnRetry: func(err error, wait time.Duration) {
			log.Warn("storage unreachable; trying again", "error", err.Error(), "retry_in", wait.String())
		},
	}
	if t := os.Getenv("ALPHEUS_S3_TIMEOUT"); t != "" {
		if s3.Timeout, err = time.ParseDuration(t); err != nil {
			return fmt.Errorf("ALPHEUS_S3_TIMEOUT: %w", err)
		}
	}
	var st store.Store
	if st, err = store.NewS3(s3); err != nil {
		return err
	}
	if b := os.Getenv("ALPHEUS_S3_RESULTS_BUCKET"); b != "" {
		s3.Bucket = b
		results, err := store.NewS3(s3)
		if err != nil {
			return err
		}
		st = store.WithResults(st, results)
	}

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
	if err := a.Run(ctx); err != nil {
		return err
	}
	if !exitOnFinish {
		<-ctx.Done()
	}
	return nil
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
