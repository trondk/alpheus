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

package builtin

import (
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"alpheus-dev.org/alpheus/internal/signal"
)

func serve(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer()
	NewHub(nil).Register(s)
	go s.Serve(lis)
	t.Cleanup(s.Stop)
	return lis.Addr().String()
}

func dial(t *testing.T, addr string) *Client {
	t.Helper()
	c, err := Dial(addr, insecure.NewCredentials(), slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// receive publishes sig until it arrives on ch, since the subscription
// reaches the hub asynchronously.
func receive(t *testing.T, pub *Client, ch <-chan signal.Signal, sig signal.Signal) signal.Signal {
	t.Helper()
	deadline := time.After(5 * time.Second)
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		pub.Publish(context.Background(), sig)
		select {
		case got := <-ch:
			return got
		case <-tick.C:
		case <-deadline:
			t.Fatalf("no %v signal", sig.Kind)
		}
	}
}

// TestForwardsPerEdge checks that the signal hub forwards a ready signal
// only to those who subscribed to its edge.
func TestForwardsPerEdge(t *testing.T) {
	addr := serve(t)
	producer, consumer := dial(t, addr), dial(t, addr)
	ctx := t.Context()

	ready, err := consumer.Subscribe(ctx, "run", "generate-filter")
	if err != nil {
		t.Fatal(err)
	}
	other, _ := consumer.Subscribe(ctx, "run", "filter-sum")

	sig := signal.Signal{Kind: signal.Ready, Run: "run", Edge: "generate-filter", Replica: 0, N: 7, Epoch: 1}
	if got := receive(t, producer, ready, sig); got != sig {
		t.Errorf("got %+v, want %+v", got, sig)
	}
	select {
	case got := <-other:
		t.Errorf("signal for another edge: %+v", got)
	default:
	}

	acks, _ := producer.Subscribe(ctx, "run", "generate-filter")
	ack := signal.Signal{Kind: signal.Ack, Run: "run", Edge: "generate-filter", N: 7, ConsumerStep: "filter", ConsumerReplica: 3}
	for got := receive(t, consumer, acks, ack); got.Kind != signal.Ack; got = <-acks {
	}
}

// A client that loses the hub reconnects and subscribes again.
func TestReconnects(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	s := grpc.NewServer()
	NewHub(nil).Register(s)
	go s.Serve(lis)

	c := dial(t, addr)
	ch, _ := c.Subscribe(t.Context(), "run", "a-b")
	sig := signal.Signal{Kind: signal.EOS, Run: "run", Edge: "a-b"}
	receive(t, c, ch, sig)

	s.Stop()
	lis, err = net.Listen("tcp", addr)
	if err != nil {
		t.Skipf("port taken after restart: %v", err)
	}
	s = grpc.NewServer()
	NewHub(nil).Register(s)
	go s.Serve(lis)
	t.Cleanup(s.Stop)

	receive(t, c, ch, sig)
}
