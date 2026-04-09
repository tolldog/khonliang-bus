// Package config loads khonliang-bus configuration from YAML.
package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the top-level configuration.
type Config struct {
	Listen   string         `yaml:"listen"`
	Backend  BackendConfig  `yaml:"backend"`
	Registry RegistryConfig `yaml:"registry"`
}

// BackendConfig selects and parameterizes the storage layer.
type BackendConfig struct {
	Type      string          `yaml:"type"` // memory | sqlite | redis
	SQLite    SQLiteConfig    `yaml:"sqlite"`
	Redis     RedisConfig     `yaml:"redis"`
	Retention RetentionConfig `yaml:"retention"`
}

// SQLiteConfig parameters for the SQLite backend.
type SQLiteConfig struct {
	Path string `yaml:"path"`
}

// RedisConfig parameters for the Redis backend (v0.2+).
type RedisConfig struct {
	URL string `yaml:"url"`
}

// RetentionConfig controls automatic message and subscriber pruning.
type RetentionConfig struct {
	MessagesTTL       time.Duration `yaml:"messages_ttl"`
	SubscriberIdleTTL time.Duration `yaml:"subscriber_idle_ttl"`
}

// RegistryConfig controls service discovery behavior.
type RegistryConfig struct {
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval"`
}

// Default returns a sensible default config (memory backend, port 8787).
func Default() Config {
	return Config{
		Listen: ":8787",
		Backend: BackendConfig{
			Type: "memory",
			Retention: RetentionConfig{
				MessagesTTL:       7 * 24 * time.Hour,
				SubscriberIdleTTL: 30 * 24 * time.Hour,
			},
		},
		Registry: RegistryConfig{
			HeartbeatInterval: 30 * time.Second,
		},
	}
}

// Load reads a YAML config file and merges it onto the defaults.
func Load(path string) (Config, error) {
	cfg := Default()
	if path == "" {
		return cfg, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, fmt.Errorf("read config: %w", err)
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return cfg, fmt.Errorf("parse config: %w", err)
	}
	return cfg, nil
}
