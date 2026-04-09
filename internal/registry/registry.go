// Package registry provides service discovery for khonliang-bus.
//
// Services register themselves on startup and send periodic heartbeats.
// Clients query the registry to discover available services.
package registry

import (
	"errors"
	"sync"
	"time"
)

// Service describes a registered application that uses the bus.
type Service struct {
	ID            string            `json:"id"`
	Name          string            `json:"name"`
	Topics        []string          `json:"topics"`        // topics this service publishes/subscribes
	Metadata      map[string]string `json:"metadata,omitempty"`
	RegisteredAt  time.Time         `json:"registered_at"`
	LastHeartbeat time.Time         `json:"last_heartbeat"`
}

// Registry is an in-memory service registry with TTL-based pruning.
type Registry struct {
	mu       sync.RWMutex
	services map[string]*Service
	ttl      time.Duration
}

// New returns a registry that prunes services not seen within ttl.
func New(ttl time.Duration) *Registry {
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	return &Registry{
		services: make(map[string]*Service),
		ttl:      ttl,
	}
}

// Register adds or updates a service entry.
func (r *Registry) Register(svc Service) error {
	if svc.ID == "" || svc.Name == "" {
		return errors.New("service id and name required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now().UTC()
	if existing, ok := r.services[svc.ID]; ok {
		existing.Name = svc.Name
		existing.Topics = svc.Topics
		existing.Metadata = svc.Metadata
		existing.LastHeartbeat = now
		return nil
	}

	svc.RegisteredAt = now
	svc.LastHeartbeat = now
	r.services[svc.ID] = &svc
	return nil
}

// Heartbeat updates the LastHeartbeat timestamp for a service.
func (r *Registry) Heartbeat(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	svc, ok := r.services[id]
	if !ok {
		return errors.New("service not registered")
	}
	svc.LastHeartbeat = time.Now().UTC()
	return nil
}

// Deregister removes a service entry.
func (r *Registry) Deregister(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.services, id)
}

// List returns all live services (those within TTL).
func (r *Registry) List() []Service {
	r.mu.RLock()
	defer r.mu.RUnlock()

	cutoff := time.Now().UTC().Add(-r.ttl)
	out := make([]Service, 0, len(r.services))
	for _, svc := range r.services {
		if svc.LastHeartbeat.Before(cutoff) {
			continue
		}
		out = append(out, *svc)
	}
	return out
}

// Get returns a single service by ID.
func (r *Registry) Get(id string) (Service, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	svc, ok := r.services[id]
	if !ok {
		return Service{}, false
	}
	return *svc, true
}

// Prune removes services whose last heartbeat is older than TTL.
// Returns the number pruned.
func (r *Registry) Prune() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	cutoff := time.Now().UTC().Add(-r.ttl)
	pruned := 0
	for id, svc := range r.services {
		if svc.LastHeartbeat.Before(cutoff) {
			delete(r.services, id)
			pruned++
		}
	}
	return pruned
}
