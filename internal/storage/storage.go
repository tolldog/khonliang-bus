// Package storage defines the backend interface for khonliang-bus.
//
// All backends (memory, sqlite, redis) implement Backend so the bus
// can swap persistence layers via configuration without core changes.
package storage

import (
	"context"
	"errors"
	"time"
)

// Message is the unit of communication on the bus.
type Message struct {
	ID        string
	Topic     string
	Payload   []byte
	Timestamp time.Time
}

// Subscription is a streaming handle returned by Subscribe.
// Callers receive messages on Messages and call Close to stop.
type Subscription interface {
	Messages() <-chan Message
	Close() error
}

// Backend is the storage interface every persistence layer implements.
//
// Semantics:
//   - Publish appends a message to the topic and returns its assigned ID.
//   - Subscribe streams messages for topic, starting after fromID.
//     Pass an empty fromID to receive only new messages.
//   - Ack records that subscriberID has processed up to msgID. The next
//     Subscribe call without an explicit fromID will resume from there.
//   - Trim removes messages older than the cutoff for the given topic.
type Backend interface {
	Publish(ctx context.Context, topic string, payload []byte) (Message, error)
	Subscribe(ctx context.Context, subscriberID, topic, fromID string) (Subscription, error)
	Ack(ctx context.Context, subscriberID, msgID string) error
	LastAcked(ctx context.Context, subscriberID, topic string) (string, error)
	Trim(ctx context.Context, topic string, olderThan time.Duration) (int, error)
	Close() error
}

// Common errors returned by backends.
var (
	ErrClosed       = errors.New("backend closed")
	ErrNotFound     = errors.New("not found")
	ErrInvalidTopic = errors.New("invalid topic")
)
