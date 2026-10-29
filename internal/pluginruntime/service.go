package pluginruntime

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
)

type Service struct {
	mu       sync.RWMutex
	adapters map[string]PluginAdapter
}

func New(adapters ...PluginAdapter) *Service {
	s := &Service{adapters: map[string]PluginAdapter{}}
	for _, adapter := range adapters {
		s.Register(adapter)
	}
	return s
}

func (s *Service) Register(adapter PluginAdapter) {
	_ = s.Replace(adapter)
}

func (s *Service) Replace(adapter PluginAdapter) PluginAdapter {
	if s == nil || adapter == nil {
		return nil
	}
	id := strings.ToLower(strings.TrimSpace(adapter.ID()))
	if id == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	previous := s.adapters[id]
	s.adapters[id] = adapter
	return previous
}

func (s *Service) Unregister(id string) PluginAdapter {
	if s == nil {
		return nil
	}
	id = strings.ToLower(strings.TrimSpace(id))
	if id == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	previous := s.adapters[id]
	delete(s.adapters, id)
	return previous
}

// Close detaches every adapter before asking it to stop. Hub shutdown calls
// this synchronously so plugin process groups cannot outlive their owner.
func (s *Service) Close(ctx context.Context) error {
	if s == nil {
		return nil
	}
	ids := s.AdapterIDs()
	var errs []error
	for _, id := range ids {
		adapter := s.Unregister(id)
		if adapter == nil {
			continue
		}
		if err := adapter.Close(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (s *Service) AdapterIDs() []string {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := make([]string, 0, len(s.adapters))
	for id := range s.adapters {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func (s *Service) Adapter(id string) PluginAdapter {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.adapters[strings.ToLower(strings.TrimSpace(id))]
}

func (s *Service) Status(ctx context.Context) []PluginStatus {
	if s == nil {
		return nil
	}
	ids := s.AdapterIDs()
	out := make([]PluginStatus, 0, len(ids))
	for _, id := range ids {
		adapter := s.Adapter(id)
		if adapter == nil {
			continue
		}
		out = append(out, s.AdapterStatus(ctx, id, adapter))
	}
	return out
}

func (s *Service) AdapterStatus(ctx context.Context, id string, adapter PluginAdapter) PluginStatus {
	id = strings.ToLower(strings.TrimSpace(id))
	if adapter == nil {
		return PluginStatus{ID: id, PluginID: id, Available: false, LastError: "plugin unavailable"}
	}
	capability, err := adapter.Probe(ctx)
	status := PluginStatus{
		ID:         id,
		PluginID:   firstNonEmpty(capability.PluginID, id),
		Available:  capability.Available,
		Surface:    capability.VisibilitySurface,
		Visible:    capability.NativeVisibleInput,
		Capability: capability,
	}
	if err != nil {
		status.Available = false
		status.LastError = err.Error()
	} else if !capability.Available {
		status.LastError = capability.UnavailableReason
	}
	if discovery, err := adapter.Discover(ctx); err == nil {
		status.Endpoint = discovery.Endpoint
		if status.Surface == "" {
			status.Surface = discovery.Surface
		}
	}
	return status
}

type AdapterHealth struct {
	ID string
	// Health is nil when the adapter does not expose passive runtime state.
	Health *RuntimeHealth
}

// HealthSnapshot returns passive supervisor health for every registered
// adapter. Unlike Status it never probes, so passive observers such as the
// local doctor can poll it freely without spawning plugin processes.
func (s *Service) HealthSnapshot() []AdapterHealth {
	if s == nil {
		return nil
	}
	ids := s.AdapterIDs()
	out := make([]AdapterHealth, 0, len(ids))
	for _, id := range ids {
		adapter := s.Adapter(id)
		if adapter == nil {
			continue
		}
		entry := AdapterHealth{ID: id}
		if reporter, ok := adapter.(RuntimeHealthReporter); ok {
			health := reporter.RuntimeHealth()
			entry.Health = &health
		}
		out = append(out, entry)
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
