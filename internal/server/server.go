// Package server exposes khonliang-bus over HTTP and WebSocket.
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/tolldog/khonliang-bus/internal/registry"
	"github.com/tolldog/khonliang-bus/internal/storage"
)

// Server wires storage + registry behind HTTP/WebSocket handlers.
type Server struct {
	backend        storage.Backend
	registry       *registry.Registry
	mux            *http.ServeMux
	allowedOrigins []string

	mu         sync.Mutex
	subsByConn map[*websocket.Conn]storage.Subscription
}

// Option configures a Server at construction time.
type Option func(*Server)

// WithAllowedOrigins sets the WebSocket origin allow-list (passed to
// websocket.AcceptOptions.OriginPatterns). Empty enforces same-host.
func WithAllowedOrigins(patterns ...string) Option {
	return func(s *Server) {
		s.allowedOrigins = patterns
	}
}

// New constructs a server with the given backend and registry.
func New(backend storage.Backend, reg *registry.Registry, opts ...Option) *Server {
	s := &Server{
		backend:    backend,
		registry:   reg,
		mux:        http.NewServeMux(),
		subsByConn: make(map[*websocket.Conn]storage.Subscription),
	}
	for _, opt := range opts {
		opt(s)
	}
	s.routes()
	return s
}

// Handler returns the underlying http.Handler.
func (s *Server) Handler() http.Handler {
	return s.mux
}

func (s *Server) routes() {
	s.mux.HandleFunc("/v1/health", s.handleHealth)
	s.mux.HandleFunc("/v1/publish", s.handlePublish)
	s.mux.HandleFunc("/v1/subscribe", s.handleSubscribe)
	s.mux.HandleFunc("/v1/ack", s.handleAck)
	s.mux.HandleFunc("/v1/register", s.handleRegister)
	s.mux.HandleFunc("/v1/services", s.handleServices)
	s.mux.HandleFunc("/v1/heartbeat", s.handleHeartbeat)
}

// --- HTTP handlers ---

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

type publishRequest struct {
	Topic   string          `json:"topic"`
	Payload json.RawMessage `json:"payload"`
}

func (s *Server) handlePublish(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req publishRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("decode: %v", err), http.StatusBadRequest)
		return
	}
	if req.Topic == "" {
		http.Error(w, "topic required", http.StatusBadRequest)
		return
	}

	msg, err := s.backend.Publish(r.Context(), req.Topic, req.Payload)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"id":        msg.ID,
		"timestamp": msg.Timestamp.Format(time.RFC3339Nano),
	})
}

type ackRequest struct {
	SubscriberID string `json:"subscriber_id"`
	MessageID    string `json:"message_id"`
}

func (s *Server) handleAck(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req ackRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("decode: %v", err), http.StatusBadRequest)
		return
	}
	if err := s.backend.Ack(r.Context(), req.SubscriberID, req.MessageID); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "acked"})
}

func (s *Server) handleRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var svc registry.Service
	if err := json.NewDecoder(r.Body).Decode(&svc); err != nil {
		http.Error(w, fmt.Sprintf("decode: %v", err), http.StatusBadRequest)
		return
	}
	if err := s.registry.Register(svc); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "registered", "id": svc.ID})
}

func (s *Server) handleServices(w http.ResponseWriter, _ *http.Request) {
	services := s.registry.List()
	writeJSON(w, http.StatusOK, services)
}

type heartbeatRequest struct {
	ID string `json:"id"`
}

func (s *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req heartbeatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("decode: %v", err), http.StatusBadRequest)
		return
	}
	if err := s.registry.Heartbeat(req.ID); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// --- WebSocket subscribe handler ---

// subscribeRequest is the first message a client sends after upgrading.
type subscribeRequest struct {
	SubscriberID string `json:"subscriber_id"`
	Topic        string `json:"topic"`
	FromID       string `json:"from_id,omitempty"`
}

// wsMessage is what the server sends back over the WebSocket.
type wsMessage struct {
	Type      string          `json:"type"` // "message" | "error"
	ID        string          `json:"id,omitempty"`
	Topic     string          `json:"topic,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	Timestamp string          `json:"timestamp,omitempty"`
	Error     string          `json:"error,omitempty"`
}

func (s *Server) handleSubscribe(w http.ResponseWriter, r *http.Request) {
	// OriginPatterns enforces same-host by default; callers from other
	// origins must be explicitly allowed via WithAllowedOrigins.
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns: s.allowedOrigins,
	})
	if err != nil {
		log.Printf("ws accept: %v", err)
		return
	}
	closeStatus := websocket.StatusNormalClosure
	closeReason := "closing"
	defer func() {
		_ = c.Close(closeStatus, closeReason)
	}()

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	var req subscribeRequest
	if err := wsjson.Read(ctx, c, &req); err != nil {
		closeStatus = websocket.StatusProtocolError
		closeReason = "missing subscribe frame"
		return
	}
	if req.SubscriberID == "" || req.Topic == "" {
		_ = wsjson.Write(ctx, c, wsMessage{Type: "error", Error: "subscriber_id and topic required"})
		closeStatus = websocket.StatusPolicyViolation
		closeReason = "missing fields"
		return
	}

	sub, err := s.backend.Subscribe(ctx, req.SubscriberID, req.Topic, req.FromID)
	if err != nil {
		_ = wsjson.Write(ctx, c, wsMessage{Type: "error", Error: err.Error()})
		closeStatus = websocket.StatusInternalError
		closeReason = "subscribe failed"
		return
	}

	s.mu.Lock()
	s.subsByConn[c] = sub
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.subsByConn, c)
		s.mu.Unlock()
		_ = sub.Close()
	}()

	// Stream messages until client disconnects or context cancels.
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-sub.Messages():
			if !ok {
				return
			}
			out := wsMessage{
				Type:      "message",
				ID:        msg.ID,
				Topic:     msg.Topic,
				Payload:   json.RawMessage(msg.Payload),
				Timestamp: msg.Timestamp.Format(time.RFC3339Nano),
			}
			if err := wsjson.Write(ctx, c, out); err != nil {
				closeStatus = websocket.StatusInternalError
				closeReason = "write failed"
				return
			}
		}
	}
}

// --- helpers ---

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// SubscriberIDFromQuery is a small convenience used by tests.
func SubscriberIDFromQuery(q string) string {
	parts := strings.Split(q, "&")
	for _, p := range parts {
		if strings.HasPrefix(p, "subscriber_id=") {
			return strings.TrimPrefix(p, "subscriber_id=")
		}
	}
	return ""
}
