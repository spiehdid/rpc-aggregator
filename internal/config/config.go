package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server         ServerConfig           `yaml:"server"`
	HealthCheck    HealthCheckConfig      `yaml:"health_check"`
	Scoring        ScoringConfig          `yaml:"scoring"`
	CircuitBreaker CircuitBreakerConfig   `yaml:"circuit_breaker"`
	Retry          RetryConfig            `yaml:"retry"`
	WebSocket      WebSocketConfig        `yaml:"websocket"`
	RateLimit      RateLimitConfig        `yaml:"rate_limit"`
	Chains         map[string]ChainConfig `yaml:"chains"`
}

type RateLimitConfig struct {
	Enabled            bool           `yaml:"enabled"`
	TrustXForwardedFor bool           `yaml:"trust_x_forwarded_for"`
	Default            RateLimitTier  `yaml:"default"`
	CleanupInterval    time.Duration  `yaml:"cleanup_interval"`
	CleanupMaxAge      time.Duration  `yaml:"cleanup_max_age"`
	APIKeys            []APIKeyConfig `yaml:"api_keys"`
}

type RateLimitTier struct {
	RequestsPerSecond float64 `yaml:"requests_per_second"`
	Burst             int     `yaml:"burst"`
}

type APIKeyConfig struct {
	Key               string  `yaml:"key"`
	Name              string  `yaml:"name"`
	RequestsPerSecond float64 `yaml:"requests_per_second"`
	Burst             int     `yaml:"burst"`
}

type ServerConfig struct {
	Listen         string        `yaml:"listen"`
	AdminListen    string        `yaml:"admin_listen"`
	RequestTimeout time.Duration `yaml:"request_timeout"`
	DrainTimeout   time.Duration `yaml:"drain_timeout"`
	AdminToken     string        `yaml:"admin_token"`
	AllowedOrigins []string      `yaml:"allowed_origins"`
}

type HealthCheckConfig struct {
	Interval time.Duration `yaml:"interval"`
	Timeout  time.Duration `yaml:"timeout"`
}

type ScoringConfig struct {
	Weights ScoringWeights `yaml:"weights"`
}

type ScoringWeights struct {
	Latency     float64 `yaml:"latency"`
	BlockHeight float64 `yaml:"block_height"`
	ErrorRate   float64 `yaml:"error_rate"`
	PaidPenalty float64 `yaml:"paid_penalty"`
}

type CircuitBreakerConfig struct {
	ErrorThreshold  float64       `yaml:"error_threshold"`
	Window          time.Duration `yaml:"window"`
	RecoveryTimeout time.Duration `yaml:"recovery_timeout"`
}

type RetryConfig struct {
	MaxAttempts    int `yaml:"max_attempts"`
	CriticalFanOut int `yaml:"critical_fan_out"`
	MaxBatchSize   int `yaml:"max_batch_size"`
}

type ChainConfig struct {
	Adapter      string          `yaml:"adapter"`
	Path         string          `yaml:"path"`
	Cache        CacheConfig     `yaml:"cache"`
	Nodes        []NodeConfig    `yaml:"nodes"`
	Finalization *bool           `yaml:"finalization"`
	Discovery    DiscoveryConfig `yaml:"discovery"`
}

type DiscoveryConfig struct {
	Enabled  bool          `yaml:"enabled"`
	ChainID  int           `yaml:"chain_id"`
	Interval time.Duration `yaml:"interval"`
}

func (c ChainConfig) IsFinalizationEnabled() bool {
	if c.Finalization == nil {
		return true
	}
	return *c.Finalization
}

type CacheConfig struct {
	DefaultTTL time.Duration `yaml:"default_ttl"`
	MaxEntries int           `yaml:"max_entries"`
	// MaxBytes bounds the summed size of cached responses (0 = no byte limit).
	MaxBytes int64       `yaml:"max_bytes"`
	Rules    []CacheRule `yaml:"rules"`
}

type CacheRule struct {
	Method    string        `yaml:"method"`
	TTL       time.Duration `yaml:"ttl"`
	Condition string        `yaml:"condition"`
}

type NodeConfig struct {
	ID     string       `yaml:"id"`
	URL    string       `yaml:"url"`
	WSURL  string       `yaml:"ws_url"`
	Weight float64      `yaml:"weight"`
	Tier   string       `yaml:"tier"`
	Budget BudgetConfig `yaml:"budget"`
}

type BudgetConfig struct {
	DailyLimit int `yaml:"daily_limit"`
}

type WebSocketConfig struct {
	MaxConnections          int           `yaml:"max_connections"`
	MaxSubscriptionsPerConn int           `yaml:"max_subscriptions_per_conn"`
	MaxTotalSubscriptions   int           `yaml:"max_total_subscriptions"`
	MaxMessageSize          int64         `yaml:"max_message_size"`
	SendQueueSize           int           `yaml:"send_queue_size"`
	WriteTimeout            time.Duration `yaml:"write_timeout"`
	UpstreamReadBuffer      int           `yaml:"upstream_read_buffer"`
	UpstreamWriteBuffer     int           `yaml:"upstream_write_buffer"`
	ConsumerReadBuffer      int           `yaml:"consumer_read_buffer"`
	ConsumerWriteBuffer     int           `yaml:"consumer_write_buffer"`
	PingInterval            time.Duration `yaml:"ping_interval"`
	PongTimeout             time.Duration `yaml:"pong_timeout"`
	ReconnectBackoff        time.Duration `yaml:"reconnect_backoff"`
	ReconnectMaxBackoff     time.Duration `yaml:"reconnect_max_backoff"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	cfg := &Config{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	setDefaults(cfg)
	if err := validate(cfg); err != nil {
		return nil, fmt.Errorf("config validation: %w", err)
	}
	return cfg, nil
}

func validate(cfg *Config) error {
	if cfg.Server.Listen == "" {
		return fmt.Errorf("server.listen must not be empty")
	}
	if cfg.Scoring.Weights.Latency < 0 {
		return fmt.Errorf("scoring.weights.latency must be >= 0")
	}
	if cfg.Scoring.Weights.BlockHeight < 0 {
		return fmt.Errorf("scoring.weights.block_height must be >= 0")
	}
	if cfg.Scoring.Weights.ErrorRate < 0 {
		return fmt.Errorf("scoring.weights.error_rate must be >= 0")
	}
	if cfg.CircuitBreaker.ErrorThreshold < 0 || cfg.CircuitBreaker.ErrorThreshold > 1 {
		return fmt.Errorf("circuit_breaker.error_threshold must be between 0 and 1, got %v", cfg.CircuitBreaker.ErrorThreshold)
	}
	if cfg.CircuitBreaker.Window <= 0 {
		return fmt.Errorf("circuit_breaker.window must be > 0")
	}
	if len(cfg.Chains) == 0 {
		return fmt.Errorf("at least one chain must be configured")
	}
	for name, chain := range cfg.Chains {
		if chain.Path == "" || chain.Path[0] != '/' {
			return fmt.Errorf("chain %q: path must be non-empty and start with '/'", name)
		}
		if chain.Adapter == "" {
			return fmt.Errorf("chain %q: adapter must not be empty", name)
		}
		if !chain.Discovery.Enabled && len(chain.Nodes) == 0 {
			return fmt.Errorf("chain %q: at least one node is required when discovery is not enabled", name)
		}
	}
	if cfg.Scoring.Weights.PaidPenalty < 0 || cfg.Scoring.Weights.PaidPenalty > 1 {
		return fmt.Errorf("scoring.weights.paid_penalty must be between 0 and 1, got %v", cfg.Scoring.Weights.PaidPenalty)
	}
	if cfg.RateLimit.Enabled && cfg.RateLimit.Default.Burst <= 0 {
		return fmt.Errorf("rate_limit.default.burst must be > 0 when rate limiting is enabled")
	}
	return nil
}

func setDefaults(cfg *Config) {
	if cfg.Server.Listen == "" {
		cfg.Server.Listen = ":8080"
	}
	if cfg.Server.AdminListen == "" {
		cfg.Server.AdminListen = ":8081"
	}
	if cfg.Server.RequestTimeout == 0 {
		cfg.Server.RequestTimeout = 30 * time.Second
	}
	if cfg.Server.DrainTimeout == 0 {
		cfg.Server.DrainTimeout = 30 * time.Second
	}
	if cfg.HealthCheck.Interval == 0 {
		cfg.HealthCheck.Interval = 5 * time.Second
	}
	if cfg.HealthCheck.Timeout == 0 {
		cfg.HealthCheck.Timeout = 3 * time.Second
	}
	if cfg.Scoring.Weights.Latency == 0 && cfg.Scoring.Weights.BlockHeight == 0 && cfg.Scoring.Weights.ErrorRate == 0 {
		cfg.Scoring.Weights.Latency = 0.4
		cfg.Scoring.Weights.BlockHeight = 0.3
		cfg.Scoring.Weights.ErrorRate = 0.3
	}
	if cfg.Scoring.Weights.PaidPenalty == 0 {
		cfg.Scoring.Weights.PaidPenalty = 0.5
	}
	if cfg.Scoring.Weights.PaidPenalty < 0 {
		cfg.Scoring.Weights.PaidPenalty = 0
	}
	if cfg.Scoring.Weights.PaidPenalty > 1 {
		cfg.Scoring.Weights.PaidPenalty = 1
	}
	if cfg.CircuitBreaker.ErrorThreshold == 0 {
		cfg.CircuitBreaker.ErrorThreshold = 0.5
	}
	if cfg.CircuitBreaker.Window == 0 {
		cfg.CircuitBreaker.Window = 30 * time.Second
	}
	if cfg.CircuitBreaker.RecoveryTimeout == 0 {
		cfg.CircuitBreaker.RecoveryTimeout = 30 * time.Second
	}
	if cfg.Retry.MaxAttempts == 0 {
		cfg.Retry.MaxAttempts = 3
	}
	if cfg.Retry.CriticalFanOut == 0 {
		cfg.Retry.CriticalFanOut = 3
	}
	if cfg.Retry.MaxBatchSize == 0 {
		cfg.Retry.MaxBatchSize = 100
	}
	if cfg.WebSocket.MaxConnections == 0 {
		cfg.WebSocket.MaxConnections = 1000
	}
	if cfg.WebSocket.MaxSubscriptionsPerConn == 0 {
		cfg.WebSocket.MaxSubscriptionsPerConn = 100
	}
	if cfg.WebSocket.MaxTotalSubscriptions == 0 {
		cfg.WebSocket.MaxTotalSubscriptions = 10000
	}
	if cfg.WebSocket.MaxMessageSize == 0 {
		cfg.WebSocket.MaxMessageSize = 65536
	}
	if cfg.WebSocket.SendQueueSize == 0 {
		cfg.WebSocket.SendQueueSize = 256
	}
	if cfg.WebSocket.WriteTimeout == 0 {
		cfg.WebSocket.WriteTimeout = 5 * time.Second
	}
	if cfg.WebSocket.UpstreamReadBuffer == 0 {
		cfg.WebSocket.UpstreamReadBuffer = 4096
	}
	if cfg.WebSocket.UpstreamWriteBuffer == 0 {
		cfg.WebSocket.UpstreamWriteBuffer = 4096
	}
	if cfg.WebSocket.ConsumerReadBuffer == 0 {
		cfg.WebSocket.ConsumerReadBuffer = 1024
	}
	if cfg.WebSocket.ConsumerWriteBuffer == 0 {
		cfg.WebSocket.ConsumerWriteBuffer = 1024
	}
	if cfg.WebSocket.PingInterval == 0 {
		cfg.WebSocket.PingInterval = 30 * time.Second
	}
	if cfg.WebSocket.PongTimeout == 0 {
		cfg.WebSocket.PongTimeout = 10 * time.Second
	}
	if cfg.WebSocket.ReconnectBackoff == 0 {
		cfg.WebSocket.ReconnectBackoff = 1 * time.Second
	}
	if cfg.WebSocket.ReconnectMaxBackoff == 0 {
		cfg.WebSocket.ReconnectMaxBackoff = 30 * time.Second
	}
	if cfg.RateLimit.Default.RequestsPerSecond == 0 {
		cfg.RateLimit.Default.RequestsPerSecond = 100
	}
	// Only apply the burst default when rate limiting is disabled; when enabled,
	// the user must supply a positive burst so validation can catch misconfiguration.
	if !cfg.RateLimit.Enabled && cfg.RateLimit.Default.Burst == 0 {
		cfg.RateLimit.Default.Burst = 200
	}
	if cfg.RateLimit.CleanupInterval == 0 {
		cfg.RateLimit.CleanupInterval = 10 * time.Minute
	}
	if cfg.RateLimit.CleanupMaxAge == 0 {
		cfg.RateLimit.CleanupMaxAge = 5 * time.Minute
	}
	for name, chain := range cfg.Chains {
		if chain.Discovery.Enabled && chain.Discovery.Interval == 0 {
			chain.Discovery.Interval = 30 * time.Minute
			cfg.Chains[name] = chain
		}
	}
}
