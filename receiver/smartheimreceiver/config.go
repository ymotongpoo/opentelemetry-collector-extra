// Copyright 2026 Yoshi Yamaguchi
// SPDX-License-Identifier: Apache-2.0

package smartheimreceiver

import (
	"errors"
	"fmt"
	"net/netip"
	"net/url"
	"strings"
	"time"

	"github.com/ymotongpoo/opentelemetry-collector-extra/receiver/smartheimreceiver/internal/metadata"
	"go.opentelemetry.io/collector/component"
)

type Config struct {
	Endpoint                      string          `mapstructure:"endpoint"`
	DeviceID                      string          `mapstructure:"device_id"`
	CollectionInterval            time.Duration   `mapstructure:"collection_interval"`
	Timeout                       time.Duration   `mapstructure:"timeout"`
	CollectBranches               bool            `mapstructure:"collect_branches"`
	Discovery                     DiscoveryConfig `mapstructure:"discovery"`
	metadata.MetricsBuilderConfig `mapstructure:",squash"`
}

type DiscoveryConfig struct {
	Enabled     bool          `mapstructure:"enabled"`
	Networks    []string      `mapstructure:"networks"`
	Interval    time.Duration `mapstructure:"interval"`
	Timeout     time.Duration `mapstructure:"timeout"`
	Concurrency int           `mapstructure:"concurrency"`
}

func createDefaultConfig() component.Config {
	return &Config{
		CollectionInterval: 30 * time.Second, Timeout: 5 * time.Second, CollectBranches: true,
		Discovery:            DiscoveryConfig{Enabled: true, Interval: time.Minute, Timeout: 3 * time.Second, Concurrency: 16},
		MetricsBuilderConfig: metadata.NewDefaultMetricsBuilderConfig(),
	}
}

func (c *Config) Validate() error {
	if c.CollectionInterval <= 0 || c.Timeout <= 0 {
		return errors.New("collection_interval and timeout must be positive")
	}
	if c.DeviceID != strings.TrimSpace(c.DeviceID) {
		return errors.New("device_id must not contain surrounding whitespace")
	}
	if c.Endpoint != "" {
		u, err := url.Parse(c.Endpoint)
		if err != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") || (u.Scheme != "http" && u.Scheme != "https") {
			return errors.New("endpoint must be an http(s) origin without credentials, path, query or fragment")
		}
		return nil
	}
	if !c.Discovery.Enabled {
		return errors.New("endpoint is required when discovery is disabled")
	}
	if c.Discovery.Interval <= 0 || c.Discovery.Timeout <= 0 || c.Discovery.Concurrency < 1 || c.Discovery.Concurrency > 32 {
		return errors.New("discovery requires positive interval and timeout, and concurrency between 1 and 32")
	}
	for _, raw := range c.Discovery.Networks {
		if _, err := discoveryPrefix(raw); err != nil {
			return err
		}
	}
	if len(c.Discovery.Networks) > 4 {
		return errors.New("discovery supports at most four /24 or smaller networks")
	}
	return nil
}

func discoveryPrefix(raw string) (netip.Prefix, error) {
	p, err := netip.ParsePrefix(raw)
	if err != nil || !p.Addr().Is4() || !p.Addr().IsPrivate() || p.Bits() < 24 || p.Bits() > 30 {
		return netip.Prefix{}, fmt.Errorf("discovery network %q must be a private IPv4 /24 through /30", raw)
	}
	return p.Masked(), nil
}
