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

package discordexporter

import (
	"errors"

	"go.opentelemetry.io/collector/component"
)

type Config struct {
	// Token is Discord application token.
	Token string `mapstructure:"token"`

	// Channel is the target Discord channel to write data to.
	Channel string `mapstructure:"channel"`
}

var _ component.Config = (*Config)(nil)

func createDeafultConfig() component.Config {
	return &Config{}
}

func (c *Config) Validate() error {
	if c.Token == "" {
		return errors.New("token must be non-empty")
	}
	if c.Channel == "" {
		return errors.New("channel must be non-empty")
	}
	return nil
}
