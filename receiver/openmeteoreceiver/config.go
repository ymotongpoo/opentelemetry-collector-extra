// Copyright 2026 Yoshi Yamaguchi
// SPDX-License-Identifier: Apache-2.0

package openmeteoreceiver

import (
	"errors"
	"math"
	"regexp"
	"time"
	_ "time/tzdata"

	"github.com/ymotongpoo/opentelemetry-collector-extra/internal/weather"
	"go.opentelemetry.io/collector/component"
)

type Config struct {
	weather.Config `mapstructure:",squash"`
	Endpoint       string   `mapstructure:"endpoint"`
	Latitude       *float64 `mapstructure:"latitude"`
	Longitude      *float64 `mapstructure:"longitude"`
	LocationID     string   `mapstructure:"location_id"`
	Timezone       string   `mapstructure:"timezone"`
	Model          string   `mapstructure:"model"`
	PastDays       int      `mapstructure:"past_days"`
	ForecastDays   int      `mapstructure:"forecast_days"`
}

func createDefaultConfig() component.Config {
	return &Config{Config: weather.DefaultConfig(time.Hour), Endpoint: "https://api.open-meteo.com/v1/forecast", Timezone: "Asia/Tokyo", Model: "best_match", PastDays: 2, ForecastDays: 2}
}

func (c *Config) Validate() error {
	if err := c.Config.Validate(); err != nil {
		return err
	}
	if err := weather.ValidateEndpoint(c.Endpoint); err != nil {
		return err
	}
	if c.Latitude == nil || c.Longitude == nil || math.IsNaN(*c.Latitude) || math.IsNaN(*c.Longitude) || math.Abs(*c.Latitude) > 90 || math.Abs(*c.Longitude) > 180 {
		return errors.New("latitude and longitude are required and must be valid coordinates")
	}
	if c.LocationID == "" {
		return errors.New("location_id is required")
	}
	if _, err := time.LoadLocation(c.Timezone); err != nil || c.Timezone == "" || c.Timezone == "Local" {
		return errors.New("timezone must be an IANA timezone")
	}
	if !regexp.MustCompile(`^[a-z0-9_]+$`).MatchString(c.Model) {
		return errors.New("model must name one model or best_match")
	}
	if c.PastDays < 0 || c.PastDays > 92 || c.ForecastDays < 1 || c.ForecastDays > 16 {
		return errors.New("past_days must be 0..92 and forecast_days 1..16")
	}
	return nil
}
