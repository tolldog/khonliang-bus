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

	// Subscribe (must happen before publish to receive live messages)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sub, err := c.Subscribe(ctx, "events", "")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()

	// Wait briefly for subscription to register on the server.
	time.Sleep(50 * time.Millisecond)

	// Publish
	id, err := c.Publish("events", []byte(`{"type":"test"}`))
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if id == "" {
		t.Fatal("expected non-empty message id")
	}

	// Receive
	select {
	case msg := <-sub.Messages():
		if !strings.Contains(string(msg.Payload), "test") {
			t.Errorf("got payload %s", msg.Payload)
		}
		if err := c.Ack(msg.ID); err != nil {
			t.Errorf("ack: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}
