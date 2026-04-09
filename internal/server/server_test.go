package server_test

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tolldog/khonliang-bus/internal/registry"
	"github.com/tolldog/khonliang-bus/internal/server"
	"github.com/tolldog/khonliang-bus/internal/storage/memory"
	"github.com/tolldog/khonliang-bus/pkg/client"
)

func TestEndToEnd(t *testing.T) {
	backend := memory.New()
	defer backend.Close()
	reg := registry.New(time.Minute)

	srv := server.New(backend, reg)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	c, err := client.New(httpSrv.URL, "test-service")
	if err != nil {
		t.Fatalf("client: %v", err)
	}

	// Register
	if err := c.Register([]string{"events"}, map[string]string{"version": "0.1.0"}); err != nil {
		t.Fatalf("register: %v", err)
	}

	services, err := c.Services()
	if err != nil {
		t.Fatalf("services: %v", err)
	}
	if len(services) != 1 {
		t.Fatalf("got %d services, want 1", len(services))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sub, err := c.Subscribe(ctx, "events", "")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()

	// Publish-and-poll until the subscription is live, instead of a
	// fixed sleep. The first publish that lands while the consumer is
	// connected wins. Bounded by an overall deadline.
	deadline := time.After(2 * time.Second)
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()

	var received client.Message
	got := false
publishLoop:
	for {
		if _, err := c.Publish("events", []byte(`{"type":"test"}`)); err != nil {
			t.Fatalf("publish: %v", err)
		}
		select {
		case received = <-sub.Messages():
			got = true
			break publishLoop
		case <-tick.C:
			continue
		case <-deadline:
			break publishLoop
		}
	}

	if !got {
		t.Fatal("timeout waiting for message")
	}
	if !strings.Contains(string(received.Payload), "test") {
		t.Errorf("got payload %s", received.Payload)
	}
	if err := c.Ack(received.ID); err != nil {
		t.Errorf("ack: %v", err)
	}
}
