// Copyright 2026 Yoshi Yamaguchi
// SPDX-License-Identifier: Apache-2.0

package jmareceiver

import (
	"errors"
	"regexp"
	"time"

	"github.com/ymotongpoo/opentelemetry-collector-extra/internal/weather"
	"go.opentelemetry.io/collector/component"
)

var japan = time.FixedZone("JST", 9*60*60)

type Config struct {
	weather.Config     `mapstructure:",squash"`
	Endpoint           string         `mapstructure:"endpoint"`
	PrecNo             string         `mapstructure:"prec_no"`
	BlockNo            string         `mapstructure:"block_no"`
	StationName        string         `mapstructure:"station_name"`
	RecentDays         int            `mapstructure:"recent_days"`
	RevisionDays       int            `mapstructure:"revision_days"`
	RevisionInterval   time.Duration  `mapstructure:"revision_interval"`
	MinRequestInterval time.Duration  `mapstructure:"min_request_interval"`
	Backfill           BackfillConfig `mapstructure:"backfill"`
}
type BackfillConfig struct {
	StartDate string `mapstructure:"start_date"`
	EndDate   string `mapstructure:"end_date"`
	RequestID string `mapstructure:"request_id"`
}

func createDefaultConfig() component.Config {
	return &Config{Config: weather.DefaultConfig(time.Hour), RecentDays: 7, RevisionDays: 30, RevisionInterval: 7 * 24 * time.Hour, MinRequestInterval: 2 * time.Second}
}

// Historical-search block IDs are four digits for AMeDAS and five for
// meteorological stations. These are not the IDs used by real-time AMeDAS JSON.
func (c *Config) isAMeDAS() bool { return len(c.BlockNo) == 4 }

func (c *Config) endpoint() string {
	if c.Endpoint != "" {
		return c.Endpoint
	}
	if c.isAMeDAS() {
		return "https://www.data.jma.go.jp/stats/etrn/view/hourly_a1.php"
	}
	return "https://www.data.jma.go.jp/stats/etrn/view/hourly_s1.php"
}

func (c *Config) Validate() error {
	if err := c.Config.Validate(); err != nil {
		return err
	}
	if err := weather.ValidateEndpoint(c.endpoint()); err != nil {
		return err
	}
	if !regexp.MustCompile(`^\d{2}$`).MatchString(c.PrecNo) || !regexp.MustCompile(`^\d{4,5}$`).MatchString(c.BlockNo) {
		return errors.New("prec_no must be two digits; block_no must be the historical-search ID (four digits for AMeDAS, five for meteorological stations)")
	}
	if c.RecentDays < 1 || c.RecentDays > 31 || c.RevisionDays < c.RecentDays || c.RevisionDays > 366 || c.RevisionInterval < 24*time.Hour || c.MinRequestInterval < 0 {
		return errors.New("invalid JMA recent/revision days or request intervals")
	}
	if c.Backfill.StartDate != "" || c.Backfill.EndDate != "" || c.Backfill.RequestID != "" {
		a, e1 := time.ParseInLocation(time.DateOnly, c.Backfill.StartDate, japan)
		b, e2 := time.ParseInLocation(time.DateOnly, c.Backfill.EndDate, japan)
		if e1 != nil || e2 != nil || b.Before(a) || b.Sub(a) > 365*24*time.Hour || c.Backfill.RequestID == "" {
			return errors.New("backfill requires start_date, end_date (inclusive, at most 366 days) and request_id")
		}
	}
	return nil
}
