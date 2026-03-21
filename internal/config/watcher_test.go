package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWatcher_CallsCallbackOnChange(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	yaml := `
chains:
  ethereum:
    adapter: evm
    path: /eth
    nodes:
      - url: https://rpc1.example.com
        weight: 1.0
`
	os.WriteFile(path, []byte(yaml), 0644)

	called := make(chan struct{}, 1)
	callback := func(cfg *Config) {
		select {
		case called <- struct{}{}:
		default:
		}
	}

	w, err := NewWatcher(path, callback)
	if err != nil {
		t.Fatalf("NewWatcher() error: %v", err)
	}
	defer w.Close()

	// Give the watcher time to start before modifying the file.
	time.Sleep(50 * time.Millisecond)

	updatedYaml := `
chains:
  ethereum:
    adapter: evm
    path: /eth
    nodes:
      - url: https://rpc1.example.com
        weight: 2.0
`
	os.WriteFile(path, []byte(updatedYaml), 0644)

	select {
	case <-called:
		// ok — callback was invoked
	case <-time.After(2 * time.Second):
		t.Error("callback not called within 2 seconds")
	}
}

func TestWatcher_InvalidConfigDoesNotCallback(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	yaml := `
chains:
  ethereum:
    adapter: evm
    path: /eth
    nodes:
      - url: https://rpc1.example.com
        weight: 1.0
`
	os.WriteFile(path, []byte(yaml), 0644)

	called := make(chan struct{}, 1)
	callback := func(cfg *Config) {
		select {
		case called <- struct{}{}:
		default:
		}
	}

	w, err := NewWatcher(path, callback)
	if err != nil {
		t.Fatalf("NewWatcher() error: %v", err)
	}
	defer w.Close()

	// Give the watcher time to start before modifying the file.
	time.Sleep(50 * time.Millisecond)

	// Write invalid YAML — callback must NOT be invoked.
	os.WriteFile(path, []byte("{{invalid"), 0644)

	// Wait briefly, then verify no callback was fired.
	select {
	case <-called:
		t.Error("callback should not be called for invalid config")
	case <-time.After(500 * time.Millisecond):
		// ok — no callback within the window
	}
}
