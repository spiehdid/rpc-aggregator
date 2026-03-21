package adapter

import (
	"fmt"
	"sync"
)

type Registry struct {
	mu       sync.RWMutex
	adapters map[string]ChainAdapter
}

func NewRegistry() *Registry {
	return &Registry{adapters: make(map[string]ChainAdapter)}
}

func (r *Registry) Register(name string, adapter ChainAdapter) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.adapters[name] = adapter
}

func (r *Registry) Get(name string) (ChainAdapter, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	a, ok := r.adapters[name]
	if !ok {
		return nil, fmt.Errorf("adapter %q not registered", name)
	}
	return a, nil
}
