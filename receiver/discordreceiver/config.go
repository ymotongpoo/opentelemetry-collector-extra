// Copyright 2023 Yoshi Yamaguchi
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package discordreceiver

import (
	"errors"
	"time"

	"github.com/ymotongpoo/opentelemetry-collector-extra/receiver/discordreceiver/internal/metadata"
	"go.opentelemetry.io/collector/component"
)

// Config defines configuration for Discord receiver.

type Config struct {
	// Token is the Discord bot token
	Token string `mapstructure:"token"`

	// Metrics is metrics related configs
	Metrics *MetricsConfig `mapstructure:"metrics"`

	// Logs is logs related configs
	Logs *LogsConfig `mapstructure:"logs"`
}

type MetricsConfig struct {
	// BufferInterval is the interval period to buffer messages from Discord.
	// Valid time units are "ns", "us" (or "µs"), "ms", "s", "m" and "h", which are
	// supported by time.ParseDuration().
	// Default is "30s".
	BufferInterval string `mapstructure:"buffer_interval"`

	// ServerWide is an optional setting to collect logs and statistics from all channels in the server.
	// If it is false, the receiver collects statistics per channel.
	// If it is true, the receiver ignores the Channels setting.
	// Default is false.
	ServerWide bool `mapstructure:"server_wide,omitempty"`

	// Channels is an optional setting to collect statistics from specific channels.
	// If it is empty, the receiver collects statistics from all channels.
	// The ServerWide setting is true, receiver ignores this setting.
	Channels []string `mapstructure:"channels,omitempty"`

	// MetricsBuilderConfig is the configuration for the metrics builder.
	MetricsBuilderConfig metadata.MetricsBuilderConfig
}

type LogsConfig struct {
	// Channels is the option to decide which logs to collect based on channels.
	// If it is empty, the receiver doesn't collect any logs.
	// Default is empty, so you need to explicitly specify the channels.
	Channels []string `mapstructure:"channels,omitempty"`
}

func createDefaultConfig() component.Config {
	mc := &MetricsConfig{
		BufferInterval:       "30s",
		ServerWide:           false,
		Channels:             []string{},
		MetricsBuilderConfig: metadata.DefaultMetricsBuilderConfig(),
	}
	lc := &LogsConfig{
		Channels: []string{},
	}
	return &Config{
		Token:   "",
		Metrics: mc,
		Logs:    lc,
	}
}

var _ component.Config = (*Config)(nil)

// Validate checks if the receiver configuration is valid.
func (c Config) Validate() error {
	if c.Token == "" {
		return errors.New("token cannot be empty")
	}
	if c.Metrics.ServerWide && len(c.Metrics.Channels) > 0 {
		return errors.New("server_wide and channels cannot be set at the same time")
	}
	if _, err := time.ParseDuration(c.Metrics.BufferInterval); err != nil {
		return err
	}
	return nil
}
