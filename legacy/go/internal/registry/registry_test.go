package registry

import (
	"testing"
	"time"
)

func TestRegisterAndList(t *testing.T) {
	r := New(time.Minute)

	if err := r.Register(Service{
		ID:     "svc1",
		Name:   "researcher",
		Topics: []string{"paper.distilled"},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	services := r.List()
	if len(services) != 1 {
		t.Fatalf("got %d services, want 1", len(services))
	}
	if services[0].ID != "svc1" {
		t.Errorf("got id %s", services[0].ID)
	}
}

func TestPruneStale(t *testing.T) {
	r := New(10 * time.Millisecond)
	_ = r.Register(Service{ID: "svc1", Name: "test"})

	time.Sleep(20 * time.Millisecond)
	pruned := r.Prune()
	if pruned != 1 {
		t.Errorf("pruned %d, want 1", pruned)
	}
}

func TestHeartbeatExtendsLife(t *testing.T) {
	r := New(50 * time.Millisecond)
	_ = r.Register(Service{ID: "svc1", Name: "test"})

	time.Sleep(30 * time.Millisecond)
	if err := r.Heartbeat("svc1"); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}

	time.Sleep(30 * time.Millisecond)
	// Total elapsed = 60ms, but heartbeat at 30ms should keep it alive.
	if pruned := r.Prune(); pruned != 0 {
		t.Errorf("pruned %d, want 0", pruned)
	}
}
