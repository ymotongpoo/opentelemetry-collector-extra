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
	"strings"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configopaque"
)

// Config defines the destination and delivery limits.
type Config struct {
	Token   configopaque.String `mapstructure:"token"`
	Channel string              `mapstructure:"channel"`
	// Timeout includes all messages, rate-limit waits, and retries for one metrics batch.
	Timeout time.Duration `mapstructure:"timeout"`
	// MaxRetries is the number of retries for each message after its first attempt.
	MaxRetries int `mapstructure:"max_retries"`
}

func createDefaultConfig() component.Config { return &Config{Timeout: 30 * time.Second, MaxRetries: 3} }

func (c *Config) Validate() error {
	if strings.TrimSpace(string(c.Token)) == "" {
		return errors.New("token must be non-empty")
	}
	if strings.TrimSpace(c.Channel) == "" {
		return errors.New("channel must be non-empty")
	}
	if strings.TrimSpace(c.Channel) != c.Channel {
		return errors.New("channel must not contain surrounding whitespace")
	}
	if c.Timeout <= 0 {
		return errors.New("timeout must be positive")
	}
	if c.MaxRetries < 0 || c.MaxRetries > 10 {
		return errors.New("max_retries must be between 0 and 10")
	}
	return nil
}
