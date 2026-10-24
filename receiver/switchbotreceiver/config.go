// Copyright 2026 Yoshi Yamaguchi
// SPDX-License-Identifier: Apache-2.0

package switchbotreceiver

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/ymotongpoo/opentelemetry-collector-extra/receiver/switchbotreceiver/internal/metadata"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configopaque"
)

// Config selects sources and collection intervals. No device commands are sent.
type Config struct {
	Cloud                         CloudConfig   `mapstructure:"cloud"`
	Matter                        MatterConfig  `mapstructure:"matter"`
	BatteryInterval               time.Duration `mapstructure:"battery_interval"`
	ExportInterval                time.Duration `mapstructure:"export_interval"`
	Devices                       []string      `mapstructure:"devices"`
	metadata.MetricsBuilderConfig `mapstructure:",squash"`
}

type CloudConfig struct {
	Enabled            bool                `mapstructure:"enabled"`
	Token              configopaque.String `mapstructure:"token"`
	Secret             configopaque.String `mapstructure:"secret"`
	Endpoint           string              `mapstructure:"endpoint"`
	CollectionInterval time.Duration       `mapstructure:"collection_interval"`
	DiscoveryInterval  time.Duration       `mapstructure:"discovery_interval"`
	Timeout            time.Duration       `mapstructure:"timeout"`
	MinRequestInterval time.Duration       `mapstructure:"min_request_interval"`
	DailyRequestLimit  int                 `mapstructure:"daily_request_limit"`
}

type MatterConfig struct {
	Enabled            bool          `mapstructure:"enabled"`
	Endpoint           string        `mapstructure:"endpoint"`
	CollectionInterval time.Duration `mapstructure:"collection_interval"`
	Timeout            time.Duration `mapstructure:"timeout"`
	Bindings           []Binding     `mapstructure:"bindings"`
}

// Binding maps one physical SwitchBot device to explicitly selected attributes.
// A bridge can put its temperature and humidity on different endpoints.
type Binding struct {
	DeviceID              string            `mapstructure:"device_id"`
	Name                  string            `mapstructure:"name"`
	DeviceType            string            `mapstructure:"device_type"`
	NodeID                uint64            `mapstructure:"node_id"`
	Attributes            map[string]string `mapstructure:"attributes"`
	ReachabilityAttribute string            `mapstructure:"reachability_attribute"`
}

func createDefaultConfig() component.Config {
	return &Config{
		Cloud:           CloudConfig{Enabled: true, Endpoint: "https://api.switch-bot.com/v1.1", CollectionInterval: 5 * time.Minute, DiscoveryInterval: time.Hour, Timeout: 10 * time.Second, MinRequestInterval: time.Second, DailyRequestLimit: 8000},
		Matter:          MatterConfig{Endpoint: "ws://127.0.0.1:5580/ws", CollectionInterval: time.Minute, Timeout: 10 * time.Second},
		BatteryInterval: 3 * time.Hour, ExportInterval: time.Minute,
		MetricsBuilderConfig: metadata.NewDefaultMetricsBuilderConfig(),
	}
}

func (c *Config) Validate() error {
	if !c.Cloud.Enabled && !c.Matter.Enabled {
		return errors.New("at least one source must be enabled")
	}
	if c.BatteryInterval <= 0 || c.ExportInterval <= 0 {
		return errors.New("battery_interval and export_interval must be positive")
	}
	if c.Cloud.Enabled {
		if strings.TrimSpace(string(c.Cloud.Token)) == "" || strings.TrimSpace(string(c.Cloud.Secret)) == "" {
			return errors.New("cloud.token and cloud.secret are required")
		}
		if err := validateURL(c.Cloud.Endpoint, "http", "https"); err != nil {
			return fmt.Errorf("cloud.endpoint: %w", err)
		}
		if c.Cloud.CollectionInterval <= 0 || c.Cloud.DiscoveryInterval <= 0 || c.Cloud.Timeout <= 0 || c.Cloud.MinRequestInterval < 0 {
			return errors.New("cloud intervals and timeout must be positive; min_request_interval cannot be negative")
		}
		if c.Cloud.DailyRequestLimit < 1 || c.Cloud.DailyRequestLimit > 10000 {
			return errors.New("cloud.daily_request_limit must be between 1 and 10000")
		}
	}
	ids := map[string]bool{}
	for _, id := range c.Devices {
		if strings.TrimSpace(id) == "" || ids[deviceKey(id)] {
			return errors.New("devices must contain unique non-empty IDs")
		}
		ids[deviceKey(id)] = true
	}
	if c.Matter.Enabled {
		if err := validateURL(c.Matter.Endpoint, "ws", "wss"); err != nil {
			return fmt.Errorf("matter.endpoint: %w", err)
		}
		if c.Matter.CollectionInterval <= 0 || c.Matter.Timeout <= 0 || len(c.Matter.Bindings) == 0 {
			return errors.New("matter requires positive intervals and at least one binding")
		}
		seen := map[string]bool{}
		for _, b := range c.Matter.Bindings {
			if strings.TrimSpace(b.DeviceID) == "" || b.NodeID == 0 || seen[deviceKey(b.DeviceID)] || len(b.Attributes) == 0 {
				return errors.New("matter bindings require unique device IDs, node_id and attributes")
			}
			seen[deviceKey(b.DeviceID)] = true
			for metric, path := range b.Attributes {
				if _, ok := matterScales[metric]; !ok {
					return fmt.Errorf("unsupported Matter metric %q", metric)
				}
				if !validPath(path) {
					return fmt.Errorf("invalid Matter attribute path for %s", metric)
				}
				if err := validateMatterMetric(metric, path); err != nil {
					return err
				}
			}
			if b.ReachabilityAttribute != "" && !validPath(b.ReachabilityAttribute) {
				return errors.New("invalid reachability_attribute")
			}
		}
	}
	return nil
}

func validateURL(raw, plain, secure string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != plain && u.Scheme != secure) {
		return errors.New("expected an endpoint without credentials, query or fragment")
	}
	if u.Scheme == plain && u.Hostname() != "localhost" && u.Hostname() != "127.0.0.1" && u.Hostname() != "::1" {
		return errors.New("unencrypted connections are allowed only on loopback")
	}
	return nil
}

func validPath(path string) bool {
	parts := strings.Split(path, "/")
	if len(parts) != 3 {
		return false
	}
	for i, p := range parts {
		bits := 32
		if i == 0 {
			bits = 16
		}
		if _, err := strconv.ParseUint(p, 10, bits); err != nil {
			return false
		}
	}
	return true
}

func deviceKey(id string) string { return strings.ToUpper(strings.TrimSpace(id)) }
