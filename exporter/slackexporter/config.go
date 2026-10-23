// Copyright 2026 Yoshi Yamaguchi
// SPDX-License-Identifier: Apache-2.0

package slackexporter

import (
	"errors"
	"strings"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configopaque"
)

// Config defines the destination and delivery limits.
type Config struct {
	BotToken configopaque.String `mapstructure:"bot_token"`
	Channel  string              `mapstructure:"channel"`
	// Timeout includes all messages, rate-limit waits, and retries for one metrics batch.
	Timeout time.Duration `mapstructure:"timeout"`
	// MaxRetries is the number of retries for each message after its first attempt.
	MaxRetries int `mapstructure:"max_retries"`
}

func createDefaultConfig() component.Config { return &Config{Timeout: 30 * time.Second, MaxRetries: 3} }

func (c *Config) Validate() error {
	if !strings.HasPrefix(string(c.BotToken), "xoxb-") {
		return errors.New("bot_token must start with xoxb-")
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
