package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func newTest(t *testing.T) *Backend {
	t.Helper()
	dir := t.TempDir()
	b, err := New(filepath.Join(dir, "bus.db"))
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

func TestPublishSubscribe(t *testing.T) {
	b := newTest(t)
	ctx := context.Background()

	sub, err := b.Subscribe(ctx, "sub1", "test", "")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()

	msg, err := b.Publish(ctx, "test", []byte(`{"hello":"world"}`))
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case received := <-sub.Messages():
		if received.ID != msg.ID {
			t.Errorf("got ID %s, want %s", received.ID, msg.ID)
		}
		if string(received.Payload) != `{"hello":"world"}` {
			t.Errorf("got payload %s", received.Payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

func TestPersistenceAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bus.db")

	b1, err := New(path)
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	ctx := context.Background()
	m1, _ := b1.Publish(ctx, "topic", []byte("first"))
	m2, _ := b1.Publish(ctx, "topic", []byte("second"))
	_ = b1.Ack(ctx, "sub1", m1.ID)
	_ = b1.Close()

	// Reopen — m2 should still be deliverable.
	b2, err := New(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer b2.Close()

	last, _ := b2.LastAcked(ctx, "sub1", "topic")
	if last != m1.ID {
		t.Errorf("LastAcked = %s, want %s", last, m1.ID)
	}

	sub, err := b2.Subscribe(ctx, "sub1", "topic", "")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()

	select {
	case msg := <-sub.Messages():
		if msg.ID != m2.ID {
			t.Errorf("got %s, want %s", msg.ID, m2.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}
}

func TestAckAndLastAcked(t *testing.T) {
	b := newTest(t)
	ctx := context.Background()

	m, err := b.Publish(ctx, "topic", []byte("test"))
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	if err := b.Ack(ctx, "sub1", m.ID); err != nil {
		t.Fatalf("ack: %v", err)
	}

	last, err := b.LastAcked(ctx, "sub1", "topic")
	if err != nil {
		t.Fatalf("last acked: %v", err)
	}
	if last != m.ID {
		t.Errorf("got %s, want %s", last, m.ID)
	}
}

func TestTrim(t *testing.T) {
	b := newTest(t)
	ctx := context.Background()

	_, _ = b.Publish(ctx, "topic", []byte("old"))
	time.Sleep(20 * time.Millisecond)

	n, err := b.Trim(ctx, "topic", 10*time.Millisecond)
	if err != nil {
		t.Fatalf("trim: %v", err)
	}
	if n != 1 {
		t.Errorf("trimmed %d, want 1", n)
	}
}
