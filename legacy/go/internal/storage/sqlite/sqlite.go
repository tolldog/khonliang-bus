// Package sqlite provides a SQLite-backed durable backend for khonliang-bus.
//
// Messages persist across restarts. Subscribers track their last-acked
// message per topic, so offline consumers receive missed messages on
// reconnect.
package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/tolldog/khonliang-bus/internal/storage"

	_ "modernc.org/sqlite"
)

const schema = `
CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    topic TEXT NOT NULL,
    payload BLOB NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_messages_topic_id ON messages(topic, id);
CREATE INDEX IF NOT EXISTS idx_messages_created_at ON messages(created_at);

CREATE TABLE IF NOT EXISTS subscriber_acks (
    subscriber_id TEXT NOT NULL,
    topic TEXT NOT NULL,
    last_acked_id INTEGER NOT NULL DEFAULT 0,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (subscriber_id, topic)
);
`

// Backend is the SQLite implementation of storage.Backend.
//
// Lock order (when both are held): subsMu before any subscription.mu.
// Publish snapshots subscribers under subsMu and delivers outside the
// lock to avoid lock-order inversion with subscription.Close().
type Backend struct {
	db     *sql.DB
	mu     sync.RWMutex
	closed bool
	subs   map[string]*subscription
	subsMu sync.Mutex
}

// New opens (or creates) a SQLite database at path and returns a Backend.
func New(path string) (*Backend, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	if _, err := db.Exec(schema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("init schema: %w", err)
	}
	// Single writer is safest for SQLite.
	db.SetMaxOpenConns(1)
	return &Backend{
		db:   db,
		subs: make(map[string]*subscription),
	}, nil
}

// Publish writes a message and notifies live subscribers on the topic.
func (b *Backend) Publish(ctx context.Context, topic string, payload []byte) (storage.Message, error) {
	if topic == "" {
		return storage.Message{}, storage.ErrInvalidTopic
	}

	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return storage.Message{}, storage.ErrClosed
	}
	b.mu.RUnlock()

	now := time.Now().UTC()
	res, err := b.db.ExecContext(ctx,
		`INSERT INTO messages (topic, payload, created_at) VALUES (?, ?, ?)`,
		topic, payload, now,
	)
	if err != nil {
		return storage.Message{}, fmt.Errorf("insert message: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return storage.Message{}, fmt.Errorf("last insert id: %w", err)
	}

	msg := storage.Message{
		ID:        strconv.FormatInt(id, 10),
		Topic:     topic,
		Payload:   append([]byte(nil), payload...),
		Timestamp: now,
	}

	// Snapshot live subscribers under subsMu, deliver after releasing.
	var live []*subscription
	b.subsMu.Lock()
	for _, sub := range b.subs {
		if sub.topic == topic && sub.isLive() {
			live = append(live, sub)
		}
	}
	b.subsMu.Unlock()

	for _, sub := range live {
		sub.deliver(msg)
	}

	return msg, nil
}

// Subscribe streams messages for the topic. If fromID is non-empty, it
// backfills messages with ID > fromID before going live. If fromID is
// empty, it resumes from the subscriber's last-acked ID for the topic
// when present; otherwise it streams only new messages.
//
// Backfill completes before the subscription is registered for live
// delivery, so reconnecting subscribers always observe backfilled
// messages strictly before live messages.
func (b *Backend) Subscribe(ctx context.Context, subscriberID, topic, fromID string) (storage.Subscription, error) {
	if topic == "" {
		return nil, storage.ErrInvalidTopic
	}

	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return nil, storage.ErrClosed
	}
	b.mu.RUnlock()

	// Validate fromID if provided.
	if fromID != "" {
		if _, err := strconv.ParseInt(fromID, 10, 64); err != nil {
			return nil, fmt.Errorf("%w: fromID %q is not a valid id", storage.ErrNotFound, fromID)
		}
	}

	// Resolve effective starting ID per the storage.Backend contract.
	start := fromID
	if start == "" {
		acked, err := b.LastAcked(ctx, subscriberID, topic)
		if err != nil {
			return nil, err
		}
		start = acked
	}
	if start == "" {
		// No prior ack: anchor to the current tail so the subscriber
		// only receives newly published messages.
		latest, err := b.maxIDForTopic(ctx, topic)
		if err != nil {
			return nil, err
		}
		start = strconv.FormatInt(latest, 10)
	}

	sub := &subscription{
		id:           fmt.Sprintf("%s:%s:%d", subscriberID, topic, time.Now().UnixNano()),
		subscriberID: subscriberID,
		topic:        topic,
		ch:           make(chan storage.Message, 64),
		backend:      b,
	}

	b.subsMu.Lock()
	b.subs[sub.id] = sub
	b.subsMu.Unlock()

	go b.backfillLoop(ctx, sub, start)
	return sub, nil
}

// backfillLoop drains pending DB messages, then loops if new ones
// arrived during the drain, and finally marks the subscription live
// under subsMu so the live transition is atomic with Publish.
//
// On any unrecoverable error, the subscription is closed so callers
// stop blocking on Messages().
func (b *Backend) backfillLoop(ctx context.Context, sub *subscription, fromID string) {
	// fromID is already validated in Subscribe; treat empty as 0.
	cursor := int64(0)
	if fromID != "" {
		// ParseInt is safe here because Subscribe rejected invalid IDs.
		if v, err := strconv.ParseInt(fromID, 10, 64); err == nil {
			cursor = v
		}
	}

	for {
		drained, latest, err := b.drainOnce(ctx, sub, cursor)
		if err != nil {
			// Unrecoverable backfill error — close the subscription so
			// the consumer doesn't block on Messages() forever.
			_ = sub.Close()
			return
		}
		if !drained {
			return
		}
		cursor = latest

		// Try to flip live atomically. If a newer message landed during
		// the drain, loop and drain again.
		b.subsMu.Lock()
		latestInDB, err := b.maxIDForTopic(ctx, sub.topic)
		if err != nil {
			b.subsMu.Unlock()
			_ = sub.Close()
			return
		}
		if latestInDB <= cursor {
			sub.markLive()
			b.subsMu.Unlock()
			return
		}
		b.subsMu.Unlock()
	}
}

// drainOnce sends every message with id > cursor to sub. Returns
// (drained, latestID, err).
//   - drained=false, err==nil: subscription closed or context cancelled
//   - drained=false, err!=nil: query/iteration error (caller should close)
//   - drained=true: all current rows delivered
func (b *Backend) drainOnce(ctx context.Context, sub *subscription, cursor int64) (bool, int64, error) {
	rows, err := b.db.QueryContext(ctx,
		`SELECT id, topic, payload, created_at FROM messages
		 WHERE topic = ? AND id > ?
		 ORDER BY id ASC`,
		sub.topic, cursor,
	)
	if err != nil {
		return false, cursor, err
	}
	defer rows.Close()

	latest := cursor
	for rows.Next() {
		var (
			id      int64
			topic   string
			payload []byte
			created time.Time
		)
		if err := rows.Scan(&id, &topic, &payload, &created); err != nil {
			return false, latest, err
		}
		msg := storage.Message{
			ID:        strconv.FormatInt(id, 10),
			Topic:     topic,
			Payload:   payload,
			Timestamp: created,
		}
		select {
		case <-ctx.Done():
			return false, latest, nil
		default:
		}
		for !sub.trySend(msg) {
			sub.mu.Lock()
			closed := sub.closed
			sub.mu.Unlock()
			if closed {
				return false, latest, nil
			}
			select {
			case <-ctx.Done():
				return false, latest, nil
			case <-time.After(5 * time.Millisecond):
			}
		}
		latest = id
	}
	if err := rows.Err(); err != nil {
		return false, latest, err
	}
	return true, latest, nil
}

// maxIDForTopic returns the highest message ID for a topic, or 0 if
// the topic is empty.
func (b *Backend) maxIDForTopic(ctx context.Context, topic string) (int64, error) {
	var id sql.NullInt64
	err := b.db.QueryRowContext(ctx,
		`SELECT MAX(id) FROM messages WHERE topic = ?`, topic,
	).Scan(&id)
	if err != nil {
		return 0, err
	}
	if !id.Valid {
		return 0, nil
	}
	return id.Int64, nil
}

// Ack records that subscriberID has processed up to msgID.
func (b *Backend) Ack(ctx context.Context, subscriberID, msgID string) error {
	if subscriberID == "" || msgID == "" {
		return storage.ErrNotFound
	}

	id, err := strconv.ParseInt(msgID, 10, 64)
	if err != nil {
		return storage.ErrNotFound
	}

	var topic string
	err = b.db.QueryRowContext(ctx,
		`SELECT topic FROM messages WHERE id = ?`, id,
	).Scan(&topic)
	if err == sql.ErrNoRows {
		return storage.ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("lookup topic: %w", err)
	}

	// Out-of-order acks must not regress last_acked_id, so only advance.
	_, err = b.db.ExecContext(ctx, `
		INSERT INTO subscriber_acks (subscriber_id, topic, last_acked_id, updated_at)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(subscriber_id, topic) DO UPDATE SET
			last_acked_id = CASE
				WHEN excluded.last_acked_id > subscriber_acks.last_acked_id
					THEN excluded.last_acked_id
				ELSE subscriber_acks.last_acked_id
			END,
			updated_at = CASE
				WHEN excluded.last_acked_id > subscriber_acks.last_acked_id
					THEN excluded.updated_at
				ELSE subscriber_acks.updated_at
			END
		`, subscriberID, topic, id, time.Now().UTC(),
	)
	if err != nil {
		return fmt.Errorf("update ack: %w", err)
	}
	return nil
}

// LastAcked returns the last acked message ID for a subscriber on a topic.
func (b *Backend) LastAcked(ctx context.Context, subscriberID, topic string) (string, error) {
	var id int64
	err := b.db.QueryRowContext(ctx,
		`SELECT last_acked_id FROM subscriber_acks WHERE subscriber_id = ? AND topic = ?`,
		subscriberID, topic,
	).Scan(&id)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("lookup ack: %w", err)
	}
	return strconv.FormatInt(id, 10), nil
}

// Topics returns all distinct topics currently in the messages table.
func (b *Backend) Topics(ctx context.Context) ([]string, error) {
	rows, err := b.db.QueryContext(ctx,
		`SELECT DISTINCT topic FROM messages ORDER BY topic`,
	)
	if err != nil {
		return nil, fmt.Errorf("list topics: %w", err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return nil, fmt.Errorf("scan topic: %w", err)
		}
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate topics: %w", err)
	}
	return out, nil
}

// Trim removes messages older than cutoff for the given topic.
func (b *Backend) Trim(ctx context.Context, topic string, olderThan time.Duration) (int, error) {
	cutoff := time.Now().UTC().Add(-olderThan)
	res, err := b.db.ExecContext(ctx,
		`DELETE FROM messages WHERE topic = ? AND created_at < ?`,
		topic, cutoff,
	)
	if err != nil {
		return 0, fmt.Errorf("trim: %w", err)
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// Close stops the backend and closes the database handle.
func (b *Backend) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	b.mu.Unlock()

	b.subsMu.Lock()
	subs := b.subs
	b.subs = nil
	b.subsMu.Unlock()

	for _, sub := range subs {
		_ = sub.Close()
	}

	return b.db.Close()
}

// subscription is a per-call streaming handle.
//
// Lock order: when both subsMu and s.mu are needed, subsMu must be
// acquired first. Close() follows this order; trySend() only takes s.mu.
type subscription struct {
	id           string
	subscriberID string
	topic        string
	ch           chan storage.Message
	backend      *Backend
	mu           sync.Mutex
	closed       bool
	live         bool
}

func (s *subscription) Messages() <-chan storage.Message {
	return s.ch
}

func (s *subscription) Close() error {
	// Acquire backend subs lock first to honor lock order with Publish.
	s.backend.subsMu.Lock()
	s.mu.Lock()
	defer s.backend.subsMu.Unlock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true
	delete(s.backend.subs, s.id)
	close(s.ch)
	return nil
}

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

func (s *subscription) deliver(msg storage.Message) {
	_ = s.trySend(msg)
}

// markLive flips the subscription into live-delivery mode. Must be
// called with subsMu held so the transition is atomic with Publish.
func (s *subscription) markLive() {
	s.mu.Lock()
	s.live = true
	s.mu.Unlock()
}

// isLive reports whether the subscription has finished backfill.
func (s *subscription) isLive() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.live
}
