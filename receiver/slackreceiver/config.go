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

package slackreceiver

import (
	"errors"
	"strings"
	"time"

	"github.com/ymotongpoo/opentelemetry-collector-extra/receiver/slackreceiver/internal/metadata"
	"go.opentelemetry.io/collector/component"
)

// Config defines configuration for Slack receiver.

type Config struct {
	// BotToken is the Slack bot token.
	BotToken string `mapstructure:"bot_token"`

	// AppToken is the Slack bot token.
	AppToken string `mapstructure:"app_token"`

	// BufferInterval is the interval period to buffer messages from Slack.
	// Valid time units are "ns", "us" (or "µs"), "ms", "s", "m" and "h", which are
	// supported by time.ParseDuration().
	// Default is "30s".
	BufferInterval string `mapstructure:"buffer_interval"`

	// ServerWide is an optional setting to collect statistics from all channels in the workspace.
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

func createDefaultConfig() component.Config {
	return &Config{
		BotToken:             "",
		AppToken:             "",
		BufferInterval:       "30s",
		ServerWide:           false,
		Channels:             []string{},
		MetricsBuilderConfig: metadata.DefaultMetricsBuilderConfig(),
	}
}

var _ component.Config = (*Config)(nil)

// Validate checks if the receiver configuration is valid.
func (c Config) Validate() error {
	if c.BotToken == "" {
		return errors.New("bot token cannot be empty")
	}
	if !strings.HasPrefix(c.BotToken, "xoxb-") {
		return errors.New("bot token must start with 'x-'")
	}
	if c.AppToken == "" {
		return errors.New("app token cannot be empty")
	}
	if !strings.HasPrefix(c.AppToken, "xapp-") {
		return errors.New("app token must start with 'xapp-'")
	}
	if c.ServerWide && len(c.Channels) > 0 {
		return errors.New("server_wide and channels cannot be set at the same time")
	}
	if _, err := time.ParseDuration(c.BufferInterval); err != nil {
		return err
	}
	return nil
}
