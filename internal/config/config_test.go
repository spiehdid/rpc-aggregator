package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadConfig(t *testing.T) {
	yaml := `
server:
  listen: ":8080"
  admin_listen: ":8081"
  request_timeout: 30s
  drain_timeout: 30s

health_check:
  interval: 5s
  timeout: 3s

scoring:
  weights:
    latency: 0.4
    block_height: 0.3
    error_rate: 0.3

circuit_breaker:
  error_threshold: 0.5
  window: 30s
  recovery_timeout: 30s

retry:
  max_attempts: 3
  critical_fan_out: 3
  max_batch_size: 100

chains:
  ethereum:
    adapter: evm
    path: /eth
    cache:
      default_ttl: 5s
      max_entries: 10000
      rules:
        - method: eth_chainId
          ttl: 0s
        - method: eth_blockNumber
          ttl: 1s
    nodes:
      - url: https://rpc1.example.com
        weight: 1.0
      - url: https://rpc2.example.com
        weight: 0.8
`
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	if cfg.Server.Listen != ":8080" {
		t.Errorf("Listen = %q, want %q", cfg.Server.Listen, ":8080")
	}
	if cfg.Server.RequestTimeout != 30*time.Second {
		t.Errorf("RequestTimeout = %v, want 30s", cfg.Server.RequestTimeout)
	}
	if cfg.Scoring.Weights.Latency != 0.4 {
		t.Errorf("Latency weight = %v, want 0.4", cfg.Scoring.Weights.Latency)
	}
	if cfg.CircuitBreaker.ErrorThreshold != 0.5 {
		t.Errorf("ErrorThreshold = %v, want 0.5", cfg.CircuitBreaker.ErrorThreshold)
	}

	chain, ok := cfg.Chains["ethereum"]
	if !ok {
		t.Fatal("missing ethereum chain")
	}
	if chain.Adapter != "evm" {
		t.Errorf("Adapter = %q, want %q", chain.Adapter, "evm")
	}
	if chain.Path != "/eth" {
		t.Errorf("Path = %q, want %q", chain.Path, "/eth")
	}
	if len(chain.Nodes) != 2 {
		t.Errorf("Nodes count = %d, want 2", len(chain.Nodes))
	}
	if len(chain.Cache.Rules) != 2 {
		t.Errorf("Cache rules count = %d, want 2", len(chain.Cache.Rules))
	}
}

func TestLoadConfig_InvalidPath(t *testing.T) {
	_, err := Load("/nonexistent/path.yaml")
	if err == nil {
		t.Error("expected error for nonexistent path")
	}
}

func TestLoadConfig_InvalidYAML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.yaml")
	if err := os.WriteFile(path, []byte("{{invalid"), 0644); err != nil {
		t.Fatal(err)
	}
	_, err := Load(path)
	if err == nil {
		t.Error("expected error for invalid YAML")
	}
}

func TestLoadConfig_WebSocketDefaults(t *testing.T) {
	yaml := `
chains:
  ethereum:
    adapter: evm
    path: /eth
    nodes:
      - url: https://rpc1.example.com
        weight: 1.0
`
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	os.WriteFile(path, []byte(yaml), 0644)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.WebSocket.MaxConnections != 1000 {
		t.Errorf("MaxConnections = %d, want 1000", cfg.WebSocket.MaxConnections)
	}
	if cfg.WebSocket.SendQueueSize != 256 {
		t.Errorf("SendQueueSize = %d, want 256", cfg.WebSocket.SendQueueSize)
	}
	if cfg.WebSocket.PingInterval != 30*time.Second {
		t.Errorf("PingInterval = %v, want 30s", cfg.WebSocket.PingInterval)
	}
}

func TestLoadConfig_Defaults(t *testing.T) {
	yaml := `
chains:
  ethereum:
    adapter: evm
    path: /eth
    nodes:
      - url: https://rpc1.example.com
        weight: 1.0
`
	dir := t.TempDir()
	path := filepath.Join(dir, "minimal.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	if cfg.Server.Listen != ":8080" {
		t.Errorf("default Listen = %q, want %q", cfg.Server.Listen, ":8080")
	}
	if cfg.Server.RequestTimeout != 30*time.Second {
		t.Errorf("default RequestTimeout = %v, want 30s", cfg.Server.RequestTimeout)
	}
	if cfg.Retry.MaxAttempts != 3 {
		t.Errorf("default MaxAttempts = %d, want 3", cfg.Retry.MaxAttempts)
	}
	if cfg.Retry.MaxBatchSize != 100 {
		t.Errorf("default MaxBatchSize = %d, want 100", cfg.Retry.MaxBatchSize)
	}
}

func TestLoadConfig_Validation(t *testing.T) {
	writeConfig := func(t *testing.T, content string) string {
		t.Helper()
		dir := t.TempDir()
		path := filepath.Join(dir, "config.yaml")
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
		return path
	}

	// Base valid config used as a template for modifications.
	valid := `
server:
  listen: ":8080"
chains:
  ethereum:
    adapter: evm
    path: /eth
    nodes:
      - url: https://rpc1.example.com
        weight: 1.0
`

	t.Run("valid config loads without error", func(t *testing.T) {
		_, err := Load(writeConfig(t, valid))
		if err != nil {
			t.Errorf("unexpected error for valid config: %v", err)
		}
	})

	t.Run("no chains", func(t *testing.T) {
		cfg := `
server:
  listen: ":8080"
`
		_, err := Load(writeConfig(t, cfg))
		if err == nil {
			t.Error("expected error when no chains configured")
		}
	})

	t.Run("chain missing path", func(t *testing.T) {
		cfg := `
chains:
  ethereum:
    adapter: evm
    nodes:
      - url: https://rpc1.example.com
        weight: 1.0
`
		_, err := Load(writeConfig(t, cfg))
		if err == nil {
			t.Error("expected error when chain path is missing")
		}
	})

	t.Run("chain path not starting with slash", func(t *testing.T) {
		cfg := `
chains:
  ethereum:
    adapter: evm
    path: eth
    nodes:
      - url: https://rpc1.example.com
        weight: 1.0
`
		_, err := Load(writeConfig(t, cfg))
		if err == nil {
			t.Error("expected error when chain path does not start with '/'")
		}
	})

	t.Run("chain missing adapter", func(t *testing.T) {
		cfg := `
chains:
  ethereum:
    path: /eth
    nodes:
      - url: https://rpc1.example.com
        weight: 1.0
`
		_, err := Load(writeConfig(t, cfg))
		if err == nil {
			t.Error("expected error when chain adapter is missing")
		}
	})

	t.Run("chain no nodes and discovery disabled", func(t *testing.T) {
		cfg := `
chains:
  ethereum:
    adapter: evm
    path: /eth
`
		_, err := Load(writeConfig(t, cfg))
		if err == nil {
			t.Error("expected error when chain has no nodes and discovery is disabled")
		}
	})

	t.Run("chain with discovery enabled and no nodes is valid", func(t *testing.T) {
		cfg := `
chains:
  ethereum:
    adapter: evm
    path: /eth
    discovery:
      enabled: true
      chain_id: 1
`
		_, err := Load(writeConfig(t, cfg))
		if err != nil {
			t.Errorf("unexpected error when discovery enabled with no nodes: %v", err)
		}
	})

	t.Run("circuit breaker error threshold out of range", func(t *testing.T) {
		cfg := `
circuit_breaker:
  error_threshold: 1.5
  window: 30s
chains:
  ethereum:
    adapter: evm
    path: /eth
    nodes:
      - url: https://rpc1.example.com
        weight: 1.0
`
		_, err := Load(writeConfig(t, cfg))
		if err == nil {
			t.Error("expected error when error_threshold > 1")
		}
	})

	t.Run("negative scoring weight", func(t *testing.T) {
		cfg := `
scoring:
  weights:
    latency: -0.1
    block_height: 0.3
    error_rate: 0.3
chains:
  ethereum:
    adapter: evm
    path: /eth
    nodes:
      - url: https://rpc1.example.com
        weight: 1.0
`
		_, err := Load(writeConfig(t, cfg))
		if err == nil {
			t.Error("expected error for negative latency weight")
		}
	})

	t.Run("rate limit enabled with zero burst", func(t *testing.T) {
		cfg := `
rate_limit:
  enabled: true
  default:
    requests_per_second: 100
    burst: 0
chains:
  ethereum:
    adapter: evm
    path: /eth
    nodes:
      - url: https://rpc1.example.com
        weight: 1.0
`
		_, err := Load(writeConfig(t, cfg))
		if err == nil {
			t.Error("expected error when rate limit enabled but burst is 0")
		}
	})
}
