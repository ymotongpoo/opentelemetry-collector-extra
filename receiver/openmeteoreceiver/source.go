// Copyright 2026 Yoshi Yamaguchi
// SPDX-License-Identifier: Apache-2.0

package openmeteoreceiver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/ymotongpoo/opentelemetry-collector-extra/internal/weather"
)

type variable struct{ name, unit, sourceUnit, aggregation string }

var hourlyVariables = []variable{
	{"shortwave_radiation", "W/m2", "W/m²", "mean"},
	{"sunshine_duration", "s", "s", "sum"},
	{"precipitation", "mm", "mm", "sum"},
	{"temperature_2m", "Cel", "°C", "instant"},
	{"cloud_cover", "%", "%", "instant"},
	{"weather_code", "1", "wmo code", "instant"},
}
var dailyVariables = []variable{
	{"sunrise", "s", "unixtime", "event_time"},
	{"sunset", "s", "unixtime", "event_time"},
	{"daylight_duration", "s", "s", "sum"},
	{"sunshine_duration", "s", "s", "sum"},
}

type response struct {
	Latitude    *float64                     `json:"latitude"`
	Longitude   *float64                     `json:"longitude"`
	Timezone    string                       `json:"timezone"`
	HourlyUnits map[string]string            `json:"hourly_units"`
	DailyUnits  map[string]string            `json:"daily_units"`
	Hourly      map[string][]json.RawMessage `json:"hourly"`
	Daily       map[string][]json.RawMessage `json:"daily"`
	Error       bool                         `json:"error"`
}

type source struct {
	cfg    *Config
	client *http.Client
}

func newSource(c *Config) *source { return &source{cfg: c, client: weather.NewHTTPClient(c.Timeout)} }

func (s *source) requestURL() string {
	u, _ := url.Parse(s.cfg.Endpoint)
	q := u.Query()
	q.Set("latitude", strconv.FormatFloat(*s.cfg.Latitude, 'f', -1, 64))
	q.Set("longitude", strconv.FormatFloat(*s.cfg.Longitude, 'f', -1, 64))
	q.Set("timezone", s.cfg.Timezone)
	q.Set("timeformat", "unixtime")
	q.Set("past_days", strconv.Itoa(s.cfg.PastDays))
	q.Set("forecast_days", strconv.Itoa(s.cfg.ForecastDays))
	if s.cfg.Model != "best_match" {
		q.Set("models", s.cfg.Model)
	}
	for kind, vars := range map[string][]variable{"hourly": hourlyVariables, "daily": dailyVariables} {
		names := make([]string, len(vars))
		for i, v := range vars {
			names[i] = v.name
		}
		q.Set(kind, strings.Join(names, ","))
	}
	u.RawQuery = q.Encode()
	return u.String()
}

func (s *source) collect(ctx context.Context, _ time.Time, _ *weather.Store, publish weather.Publish) error {
	data, err := weather.Get(ctx, s.client, s.requestURL())
	if err != nil {
		return err
	}
	at := time.Now().UTC()
	records, err := parseResponse(data, s.cfg)
	if err != nil {
		return err
	}
	return publish(records, at)
}

func parseResponse(data []byte, cfg *Config) ([]weather.Record, error) {
	var resp response
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, errors.New("invalid Open-Meteo JSON")
	}
	if resp.Error || len(resp.Hourly["time"]) == 0 || len(resp.Daily["time"]) == 0 {
		return nil, errors.New("Open-Meteo returned no hourly/daily data")
	}
	if resp.Latitude == nil || resp.Longitude == nil || math.Abs(*resp.Latitude) > 90 || math.Abs(*resp.Longitude) > 180 {
		return nil, errors.New("invalid Open-Meteo grid coordinates")
	}
	if resp.HourlyUnits["time"] != "unixtime" || resp.DailyUnits["time"] != "unixtime" {
		return nil, errors.New("unexpected Open-Meteo time units")
	}
	loc, err := time.LoadLocation(cfg.Timezone)
	if err != nil {
		return nil, err
	}
	details := map[string]string{"grid_latitude": strconv.FormatFloat(*resp.Latitude, 'f', -1, 64), "grid_longitude": strconv.FormatFloat(*resp.Longitude, 'f', -1, 64), "timezone": cfg.Timezone, "model_selection": cfg.Model}
	var records []weather.Record
	for _, section := range []struct {
		data  map[string][]json.RawMessage
		units map[string]string
		vars  []variable
		daily bool
	}{
		{resp.Hourly, resp.HourlyUnits, hourlyVariables, false}, {resp.Daily, resp.DailyUnits, dailyVariables, true},
	} {
		for _, v := range section.vars {
			values, ok := section.data[v.name]
			if !ok || len(values) != len(section.data["time"]) || section.units[v.name] != v.sourceUnit {
				return nil, fmt.Errorf("Open-Meteo field %s has missing values or unexpected units", v.name)
			}
			var prev int64
			for i, raw := range values {
				var stamp int64
				if err = json.Unmarshal(section.data["time"][i], &stamp); err != nil || stamp <= 0 || (i > 0 && stamp <= prev) {
					return nil, errors.New("invalid Open-Meteo time axis")
				}
				prev = stamp
				end := time.Unix(stamp, 0).UTC()
				start := end
				if section.daily {
					day := end.In(loc)
					if day.Hour() != 0 || day.Minute() != 0 || day.Second() != 0 {
						return nil, errors.New("Open-Meteo daily timestamp is not local midnight")
					}
					end = day.AddDate(0, 0, 1).UTC()
				} else if v.aggregation != "instant" {
					start = end.Add(-time.Hour)
				}
				var value *float64
				if err = json.Unmarshal(raw, &value); err != nil {
					return nil, fmt.Errorf("invalid Open-Meteo value for %s", v.name)
				}
				quality := "model"
				kind := "model"
				if v.name == "sunrise" || v.name == "sunset" || v.name == "daylight_duration" {
					quality = "calculated"
					kind = "astronomical"
				}
				if value == nil {
					quality = "missing"
				}
				r := weather.Record{Source: "openmeteo", Kind: kind, LocationID: cfg.LocationID, Model: cfg.Model, Variable: v.name, PeriodStart: start, PeriodEnd: end, Aggregation: v.aggregation, Value: value, Unit: v.unit, Quality: quality, Details: details}
				records = append(records, r)
			}
		}
	}
	return records, nil
}
