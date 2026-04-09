package memory

import (
	"context"
	"testing"
	"time"
)

func TestPublishSubscribe(t *testing.T) {
	b := New()
	defer b.Close()

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
	if msg.ID == "" {
		t.Fatal("expected non-empty message ID")
	}

	select {
	case received := <-sub.Messages():
		if received.ID != msg.ID {
			t.Errorf("got ID %s, want %s", received.ID, msg.ID)
		}
		if string(received.Payload) != `{"hello":"world"}` {
			t.Errorf("got payload %s", received.Payload)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for message")
	}
}

func TestBackfillFromID(t *testing.T) {
	b := New()
	defer b.Close()
	ctx := context.Background()

	// Publish three messages before any subscriber exists.
	m1, _ := b.Publish(ctx, "topic", []byte("1"))
	m2, _ := b.Publish(ctx, "topic", []byte("2"))
	_, _ = b.Publish(ctx, "topic", []byte("3"))

	// Subscribe starting after m1 — should get m2 and m3.
	sub, err := b.Subscribe(ctx, "sub1", "topic", m1.ID)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()

	got := make([]string, 0, 2)
	timeout := time.After(time.Second)
	for len(got) < 2 {
		select {
		case msg := <-sub.Messages():
			got = append(got, string(msg.Payload))
		case <-timeout:
			t.Fatalf("timeout waiting for backfill, got %v", got)
		}
	}
	if got[0] != "2" || got[1] != "3" {
		t.Errorf("got %v, want [2 3]", got)
	}
	_ = m2 // silence
}

func TestAckAndResume(t *testing.T) {
	b := New()
	defer b.Close()
	ctx := context.Background()

	m1, _ := b.Publish(ctx, "topic", []byte("a"))
	m2, _ := b.Publish(ctx, "topic", []byte("b"))

	if err := b.Ack(ctx, "sub1", m1.ID); err != nil {
		t.Fatalf("ack: %v", err)
	}

	last, _ := b.LastAcked(ctx, "sub1", "topic")
	if last != m1.ID {
		t.Errorf("LastAcked = %s, want %s", last, m1.ID)
	}

	// Subscribe with empty fromID — should resume after m1, get only m2.
	sub, err := b.Subscribe(ctx, "sub1", "topic", "")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()

	select {
	case msg := <-sub.Messages():
		if msg.ID != m2.ID {
			t.Errorf("got %s, want %s", msg.ID, m2.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}

func TestTrim(t *testing.T) {
	b := New()
	defer b.Close()
	ctx := context.Background()

	_, _ = b.Publish(ctx, "topic", []byte("old"))
	time.Sleep(10 * time.Millisecond)

	n, err := b.Trim(ctx, "topic", 5*time.Millisecond)
	if err != nil {
		t.Fatalf("trim: %v", err)
	}
	if n != 1 {
		t.Errorf("trimmed %d, want 1", n)
	}
}
