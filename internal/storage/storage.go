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
//     If fromID is empty, the backend resumes after the last message
//     acknowledged by subscriberID for that topic (see LastAcked).
//     If no ack exists, the subscriber starts at the current tail and
//     receives only newly published messages.
//   - Ack records that subscriberID has processed up to msgID. The next
//     Subscribe call with an empty fromID will resume from there.
//   - Trim removes messages older than the cutoff for the given topic.
//   - Backends MUST deliver backfilled messages strictly before any
//     newly-published live messages so reconnecting subscribers observe
//     a single in-order stream.
type Backend interface {
	Publish(ctx context.Context, topic string, payload []byte) (Message, error)
	Subscribe(ctx context.Context, subscriberID, topic, fromID string) (Subscription, error)
	Ack(ctx context.Context, subscriberID, msgID string) error
	LastAcked(ctx context.Context, subscriberID, topic string) (string, error)
	Topics(ctx context.Context) ([]string, error)
	Trim(ctx context.Context, topic string, olderThan time.Duration) (int, error)
	Close() error
}

// Common errors returned by backends.
var (
	ErrClosed       = errors.New("backend closed")
	ErrNotFound     = errors.New("not found")
	ErrInvalidTopic = errors.New("invalid topic")
)
