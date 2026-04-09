// Package memory provides an in-memory backend for khonliang-bus.
//
// All state is lost when the process exits. Suitable for development,
// testing, and short-lived ephemeral deployments.
package memory

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/tolldog/khonliang-bus/internal/storage"
)

// Backend is an in-memory implementation of storage.Backend.
type Backend struct {
	mu       sync.RWMutex
	closed   bool
	nextID   uint64
	messages map[string][]storage.Message     // topic -> messages
	acks     map[string]map[string]string     // subscriberID -> topic -> last_acked_id
	subs     map[string]*subscription         // subscription handle registry
}

// New returns a fresh in-memory backend.
func New() *Backend {
	return &Backend{
		messages: make(map[string][]storage.Message),
		acks:     make(map[string]map[string]string),
		subs:     make(map[string]*subscription),
	}
}

// Publish appends a message to the topic and notifies live subscribers.
func (b *Backend) Publish(_ context.Context, topic string, payload []byte) (storage.Message, error) {
	if topic == "" {
		return storage.Message{}, storage.ErrInvalidTopic
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return storage.Message{}, storage.ErrClosed
	}

	b.nextID++
	msg := storage.Message{
		ID:        strconv.FormatUint(b.nextID, 10),
		Topic:     topic,
		Payload:   append([]byte(nil), payload...),
		Timestamp: time.Now().UTC(),
	}
	b.messages[topic] = append(b.messages[topic], msg)

	// Fan out to live subscribers on this topic.
	for _, sub := range b.subs {
		if sub.topic == topic {
			sub.deliver(msg)
		}
	}

	return msg, nil
}

// Subscribe streams messages for the topic. fromID="" means only new messages.
// Otherwise it backfills any messages with ID > fromID before streaming live.
func (b *Backend) Subscribe(ctx context.Context, subscriberID, topic, fromID string) (storage.Subscription, error) {
	if topic == "" {
		return nil, storage.ErrInvalidTopic
	}

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, storage.ErrClosed
	}

	// Resolve effective starting ID.
	start := fromID
	if start == "" {
		// Use last acked if known, else only new messages.
		if topicAcks, ok := b.acks[subscriberID]; ok {
			start = topicAcks[topic]
		}
	}

	sub := &subscription{
		id:           fmt.Sprintf("%s:%s:%d", subscriberID, topic, b.nextID+1),
		subscriberID: subscriberID,
		topic:        topic,
		ch:           make(chan storage.Message, 64),
		backend:      b,
	}
	b.subs[sub.id] = sub
	b.mu.Unlock()

	// Backfill missed messages outside the lock.
	go b.backfill(ctx, sub, start)

	return sub, nil
}

func (b *Backend) backfill(ctx context.Context, sub *subscription, fromID string) {
	b.mu.RLock()
	msgs := b.messages[sub.topic]
	// Snapshot to avoid holding lock during delivery.
	snapshot := make([]storage.Message, len(msgs))
	copy(snapshot, msgs)
	b.mu.RUnlock()

	for _, msg := range snapshot {
		if fromID != "" && !idGreater(msg.ID, fromID) {
			continue
		}
		select {
		case <-ctx.Done():
			return
		default:
		}
		// Bounded retry: spin briefly if buffer is full, give up if closed.
		for !sub.trySend(msg) {
			sub.mu.Lock()
			closed := sub.closed
			sub.mu.Unlock()
			if closed {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Millisecond):
			}
		}
	}
}

// Ack records the last delivered message for the subscriber.
func (b *Backend) Ack(_ context.Context, subscriberID, msgID string) error {
	if subscriberID == "" || msgID == "" {
		return storage.ErrNotFound
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return storage.ErrClosed
	}

	// Find topic for this msgID.
	for topic, msgs := range b.messages {
		for _, m := range msgs {
			if m.ID == msgID {
				if b.acks[subscriberID] == nil {
					b.acks[subscriberID] = make(map[string]string)
				}
				b.acks[subscriberID][topic] = msgID
				return nil
			}
		}
	}
	return storage.ErrNotFound
}

// LastAcked returns the last acked message ID for subscriber on topic.
func (b *Backend) LastAcked(_ context.Context, subscriberID, topic string) (string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if topicAcks, ok := b.acks[subscriberID]; ok {
		return topicAcks[topic], nil
	}
	return "", nil
}

// Trim removes messages older than cutoff for the topic.
func (b *Backend) Trim(_ context.Context, topic string, olderThan time.Duration) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return 0, storage.ErrClosed
	}

	cutoff := time.Now().UTC().Add(-olderThan)
	msgs := b.messages[topic]
	kept := msgs[:0]
	removed := 0
	for _, m := range msgs {
		if m.Timestamp.Before(cutoff) {
			removed++
			continue
		}
		kept = append(kept, m)
	}
	b.messages[topic] = kept

	// Sort kept slice by ID just in case.
	sort.SliceStable(b.messages[topic], func(i, j int) bool {
		return idLess(b.messages[topic][i].ID, b.messages[topic][j].ID)
	})

	return removed, nil
}

// Close marks the backend closed and disconnects all subscribers.
func (b *Backend) Close() error {
	b.mu.Lock()
	subs := b.subs
	b.closed = true
	b.subs = nil
	b.mu.Unlock()

	for _, sub := range subs {
		_ = sub.Close()
	}
	return nil
}

// subscription is the per-subscriber stream handle.
type subscription struct {
	id           string
	subscriberID string
	topic        string
	ch           chan storage.Message
	backend      *Backend
	mu           sync.Mutex
	closed       bool
}

func (s *subscription) Messages() <-chan storage.Message {
	return s.ch
}

func (s *subscription) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	close(s.ch)
	s.backend.mu.Lock()
	delete(s.backend.subs, s.id)
	s.backend.mu.Unlock()
	return nil
}

// trySend delivers a message under lock so a concurrent Close() cannot
// close the channel while we're sending. Returns false if the
// subscription is already closed or the buffer is full.
func (s *subscription) trySend(msg storage.Message) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	select {
	case s.ch <- msg:
		return true
	default:
		return false
	}
}

// deliver is called by Publish for live fan-out. Drops on the floor if
// the subscriber is closed or slow.
func (s *subscription) deliver(msg storage.Message) {
	_ = s.trySend(msg)
}

// idLess compares numeric message IDs.
func idLess(a, b string) bool {
	ai, _ := strconv.ParseUint(a, 10, 64)
	bi, _ := strconv.ParseUint(b, 10, 64)
	return ai < bi
}

func idGreater(a, b string) bool {
	ai, _ := strconv.ParseUint(a, 10, 64)
	bi, _ := strconv.ParseUint(b, 10, 64)
	return ai > bi
}
