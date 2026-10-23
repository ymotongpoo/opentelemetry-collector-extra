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
	"fmt"
	"strings"
	"time"

	"github.com/ymotongpoo/opentelemetry-collector-extra/receiver/slackreceiver/internal/metadata"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configopaque"
)

// Config defines the receiver configuration.
type Config struct {
	// BotToken authenticates the installed bot.
	BotToken configopaque.String `mapstructure:"bot_token"`
	// AppToken authorizes Socket Mode connections.
	AppToken configopaque.String `mapstructure:"app_token"`

	// BufferInterval controls how often cumulative metrics are emitted.
	BufferInterval time.Duration `mapstructure:"buffer_interval"`
	// ServerWide aggregates channels within each server or workspace.
	ServerWide bool `mapstructure:"server_wide"`
	// Channels contains channel IDs. An empty list includes every visible channel.
	Channels []string `mapstructure:"channels"`
	// IncludeBots includes bot messages. Keep false when exporting to a monitored channel.
	IncludeBots                   bool `mapstructure:"include_bots"`
	metadata.MetricsBuilderConfig `mapstructure:",squash"`
}

func createDefaultConfig() component.Config {
	return &Config{BufferInterval: 30 * time.Second, MetricsBuilderConfig: metadata.NewDefaultMetricsBuilderConfig()}
}

func (c *Config) Validate() error {
	if !strings.HasPrefix(string(c.BotToken), "xoxb-") {
		return errors.New("bot_token must start with xoxb-")
	}
	if !strings.HasPrefix(string(c.AppToken), "xapp-") {
		return errors.New("app_token must start with xapp-")
	}
	if c.BufferInterval <= 0 {
		return errors.New("buffer_interval must be positive")
	}
	if c.ServerWide && len(c.Channels) > 0 {
		return errors.New("server_wide and channels cannot be set at the same time")
	}
	for _, channel := range c.Channels {
		if strings.TrimSpace(channel) == "" {
			return errors.New("channels must contain non-empty channel IDs")
		}
		if strings.TrimSpace(channel) != channel {
			return fmt.Errorf("channel ID %q contains surrounding whitespace", channel)
		}
	}
	return nil
}
