// Package client is a Go client for khonliang-bus.
//
// Usage:
//
//	bus, err := client.New("http://localhost:8787", "my-service")
//	if err != nil { ... }
//
//	// Register so the bus knows about us
//	bus.Register([]string{"paper.distilled"}, nil)
//
//	// Subscribe and handle messages
//	sub, _ := bus.Subscribe(ctx, "paper.distilled", "")
//	for msg := range sub.Messages() {
//	    handle(msg)
//	    bus.Ack(msg.ID)
//	}
//
//	// Publish
//	bus.Publish("fr.updated", payload)
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

// Message is what a subscriber receives.
type Message struct {
	ID        string          `json:"id"`
	Topic     string          `json:"topic"`
	Payload   json.RawMessage `json:"payload"`
	Timestamp string          `json:"timestamp"`
}

// Client is a khonliang-bus client.
type Client struct {
	baseURL      string
	subscriberID string
	httpClient   *http.Client
}

// New constructs a client. baseURL is the bus HTTP endpoint
// (e.g. "http://localhost:8787"). subscriberID identifies this
// service for ack tracking and registration.
func New(baseURL, subscriberID string) (*Client, error) {
	if baseURL == "" || subscriberID == "" {
		return nil, errors.New("baseURL and subscriberID required")
	}
	return &Client{
		baseURL:      strings.TrimRight(baseURL, "/"),
		subscriberID: subscriberID,
		httpClient:   &http.Client{Timeout: 30 * time.Second},
	}, nil
}

// Register announces this service to the bus registry.
func (c *Client) Register(topics []string, metadata map[string]string) error {
	body := map[string]any{
		"id":       c.subscriberID,
		"name":     c.subscriberID,
		"topics":   topics,
		"metadata": metadata,
	}
	return c.postJSON("/v1/register", body, nil)
}

// Heartbeat refreshes the registry's last_seen timestamp.
func (c *Client) Heartbeat() error {
	return c.postJSON("/v1/heartbeat", map[string]string{"id": c.subscriberID}, nil)
}

// Publish sends a message to the topic.
func (c *Client) Publish(topic string, payload []byte) (string, error) {
	body := map[string]any{
		"topic":   topic,
		"payload": json.RawMessage(payload),
	}
	var resp struct {
		ID string `json:"id"`
	}
	if err := c.postJSON("/v1/publish", body, &resp); err != nil {
		return "", err
	}
	return resp.ID, nil
}

// Ack acknowledges a delivered message.
func (c *Client) Ack(messageID string) error {
	body := map[string]string{
		"subscriber_id": c.subscriberID,
		"message_id":    messageID,
	}
	return c.postJSON("/v1/ack", body, nil)
}

// Subscription is a streaming subscription to a topic.
type Subscription struct {
	conn     *websocket.Conn
	messages chan Message
	cancel   context.CancelFunc
	done     chan struct{}
}

// Messages returns a channel of incoming messages. Closed when the
// subscription ends.
func (s *Subscription) Messages() <-chan Message {
	return s.messages
}

// Close terminates the subscription.
func (s *Subscription) Close() error {
	s.cancel()
	<-s.done
	return s.conn.Close(websocket.StatusNormalClosure, "client closing")
}

// Subscribe opens a WebSocket subscription. Pass an empty fromID to
// resume from the last acked message (or only new messages if none).
func (c *Client) Subscribe(ctx context.Context, topic, fromID string) (*Subscription, error) {
	wsURL, err := httpToWS(c.baseURL + "/v1/subscribe")
	if err != nil {
		return nil, err
	}

	subCtx, cancel := context.WithCancel(ctx)
	conn, _, err := websocket.Dial(subCtx, wsURL, nil)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("ws dial: %w", err)
	}

	// Send subscribe request as the first frame.
	req := map[string]string{
		"subscriber_id": c.subscriberID,
		"topic":         topic,
		"from_id":       fromID,
	}
	if err := wsjson.Write(subCtx, conn, req); err != nil {
		_ = conn.Close(websocket.StatusInternalError, "write subscribe")
		cancel()
		return nil, fmt.Errorf("ws write subscribe: %w", err)
	}

	sub := &Subscription{
		conn:     conn,
		messages: make(chan Message, 64),
		cancel:   cancel,
		done:     make(chan struct{}),
	}

	go sub.readLoop(subCtx)
	return sub, nil
}

func (s *Subscription) readLoop(ctx context.Context) {
	// Always release resources when the loop exits, even on error or
	// remote disconnect, so callers don't have to remember to Close().
	defer close(s.messages)
	defer close(s.done)
	defer func() {
		_ = s.conn.Close(websocket.StatusNormalClosure, "read loop exit")
	}()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		var msg Message
		if err := wsjson.Read(ctx, s.conn, &msg); err != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case s.messages <- msg:
		}
	}
}

// Services returns the list of registered services from the bus registry.
func (c *Client) Services() ([]map[string]any, error) {
	resp, err := c.httpClient.Get(c.baseURL + "/v1/services")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("services: %s", string(body))
	}
	var out []map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *Client) postJSON(path string, body any, out any) error {
	buf, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, c.baseURL+path, bytes.NewReader(buf))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return fmt.Errorf("%s: %s", path, string(respBody))
	}
	if out != nil {
		return json.Unmarshal(respBody, out)
	}
	return nil
}

func httpToWS(u string) (string, error) {
	parsed, err := url.Parse(u)
	if err != nil {
		return "", err
	}
	switch parsed.Scheme {
	case "http":
		parsed.Scheme = "ws"
	case "https":
		parsed.Scheme = "wss"
	}
	return parsed.String(), nil
}
