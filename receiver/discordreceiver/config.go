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

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
)

// Config defines configuration for Discord receiver.

type Config struct {
	// Token is the Discord bot token.
	Token string `mapstructure:"token"`

	// ServerWide is an optional setting to collect statistics from all channels in the server.
	// If it is false, the receiver collects statistics per channel.
	// If it is true, the receiver ignores the Channels setting.
	// Default is false.
	ServerWide bool `mapstructure:"server_wide"`

	// Channels is an optional setting to collect statistics from specific channels.
	// If it is empty, the receiver collects statistics from all channels.
	// The ServerWide setting is true, receiver ignores this setting.
	Channels []string `mapstructure:"channels"`
}

func createDefaultConfig() component.Config {
	return &Config{
		Token:      "",
		ServerWide: false,
		Channels:   []string{},
	}
}

var _ component.Config = (*Config)(nil)
var _ confmap.Unmarshaler = (*Config)(nil)

// Validate checks if the receiver configuration is valid.
func (c Config) Validate() error {
	if c.Token == "" {
		return errors.New("token cannot be empty")
	}
	if c.ServerWide && len(c.Channels) > 0 {
		return errors.New("server_wide and channels cannot be set at the same time")
	}
	return nil
}

// Unmarshal parses the receiver configuration.
func (c *Config) Unmarshal(conf *confmap.Conf) error {
	err := conf.Unmarshal(c, confmap.WithErrorUnused(true))
	if err != nil {
		return err
	}

	if !conf.IsSet("token") {
		return errors.New("token is required")
	}

	return nil
}
