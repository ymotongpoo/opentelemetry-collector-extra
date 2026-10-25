// Copyright 2026 Yoshi Yamaguchi
// SPDX-License-Identifier: Apache-2.0

package jmareceiver

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ymotongpoo/opentelemetry-collector-extra/internal/weather"
	"golang.org/x/net/html"
)

func attr(n *html.Node, key string) string {
	for _, a := range n.Attr {
		if a.Key == key {
			return a.Val
		}
	}
	return ""
}
func nodeText(n *html.Node) string {
	if n.Type == html.TextNode {
		return n.Data
	}
	if n.Type == html.ElementNode && n.Data == "img" {
		return attr(n, "alt")
	}
	var b strings.Builder
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		b.WriteString(nodeText(c))
	}
	return strings.TrimSpace(b.String())
}
func visit(n *html.Node, f func(*html.Node)) {
	f(n)
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		visit(c, f)
	}
}
func compact(s string) string { return strings.Join(strings.Fields(s), "") }

type numericField struct {
	col                     int
	name, unit, aggregation string
	factor                  float64
}

type tableSchema struct {
	headers, subheaders []string
	groups              []int // First-row cells spanning two columns.
	columns, weatherCol int
	fields              []numericField
}

var stationSchema = tableSchema{
	headers:    []string{"時", "気圧(hPa)", "降水量(mm)", "気温(℃)", "露点温度(℃)", "蒸気圧(hPa)", "湿度(％)", "風向・風速(m/s)", "日照時間(h)", "全天日射量(MJ/㎡)", "雪(cm)", "天気", "雲量", "視程(km)"},
	subheaders: []string{"現地", "海面", "風速", "風向", "降雪", "積雪"},
	groups:     []int{1, 7, 10}, columns: 17, weatherCol: 14,
	fields: []numericField{
		{3, "precipitation", "mm", "sum", 1}, {4, "air_temperature", "Cel", "instant", 1},
		{10, "sunshine_duration", "s", "sum", 3600}, {11, "shortwave_radiation", "W/m2", "mean", 1e6 / 3600},
		{15, "cloud_cover", "%", "instant", 10},
	},
}

var amedasSchema = tableSchema{
	headers:    []string{"時", "降水量(mm)", "気温(℃)", "露点温度(℃)", "蒸気圧(hPa)", "湿度(％)", "風速・風向", "日照時間(h)", "雪"},
	subheaders: []string{"平均風速(m/s)", "風向", "降雪(cm)", "積雪(cm)"},
	groups:     []int{6, 8}, columns: 11, weatherCol: -1,
	fields: []numericField{
		{1, "precipitation", "mm", "sum", 1}, {2, "air_temperature", "Cel", "instant", 1},
		{8, "sunshine_duration", "s", "sum", 3600},
	},
}

var amedasSunshineEstimateStart = time.Date(2021, 3, 2, 0, 0, 0, 0, japan)

func span(n *html.Node, name string) int {
	value := attr(n, name)
	if value == "" {
		return 1
	}
	v, _ := strconv.Atoi(value)
	return v
}

func (s tableSchema) validateHeaders(rows []*html.Node) bool {
	for row, want := range [][]string{s.headers, s.subheaders} {
		i := 0
		for n := rows[row].FirstChild; n != nil; n = n.NextSibling {
			if n.Type != html.ElementNode {
				continue
			}
			if n.Data != "th" || i >= len(want) || compact(nodeText(n)) != want[i] {
				return false
			}
			r, c := 1, 1
			if row == 0 {
				if slices.Contains(s.groups, i) {
					c = 2
				} else {
					r = 2
				}
			}
			if span(n, "rowspan") != r || span(n, "colspan") != c {
				return false
			}
			i++
		}
		if i != len(want) {
			return false
		}
	}
	return true
}

func parsePage(data []byte, day time.Time, cfg *Config) ([]weather.Record, error) {
	doc, err := html.Parse(bytes.NewReader(data))
	if err != nil {
		return nil, errors.New("invalid JMA HTML")
	}
	var table *html.Node
	var headings []string
	visit(doc, func(n *html.Node) {
		if n.Type == html.ElementNode {
			if n.Data == "table" && attr(n, "id") == "tablefix1" {
				table = n
			}
			if n.Data == "h3" {
				headings = append(headings, nodeText(n))
			}
		}
	})
	date := fmt.Sprintf("%d年%d月%d日", day.Year(), day.Month(), day.Day())
	matched := false
	for _, h := range headings {
		if strings.Contains(h, date) && strings.Contains(h, "１時間ごとの値") && (cfg.StationName == "" || strings.HasPrefix(h, cfg.StationName+"（")) {
			matched = true
		}
	}
	if table == nil || !matched {
		return nil, errors.New("JMA hourly page is unavailable or has an unexpected date/station")
	}
	var rows []*html.Node
	visit(table, func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "tr" {
			rows = append(rows, n)
		}
	})
	if len(rows) != 26 {
		return nil, errors.New("JMA hourly table must contain two header rows and 24 hours")
	}
	schema := stationSchema
	if cfg.isAMeDAS() {
		schema = amedasSchema
	}
	if !schema.validateHeaders(rows[:2]) {
		return nil, errors.New("JMA hourly column schema changed")
	}
	var records []weather.Record
	for hour, row := range rows[2:] {
		var cells []string
		for n := row.FirstChild; n != nil; n = n.NextSibling {
			if n.Type == html.ElementNode && n.Data == "td" {
				if span(n, "rowspan") != 1 || span(n, "colspan") != 1 {
					return nil, errors.New("JMA hourly data contains merged cells")
				}
				cells = append(cells, nodeText(n))
			}
		}
		if len(cells) != schema.columns || cells[0] != strconv.Itoa(hour+1) {
			return nil, errors.New("JMA hourly row is missing, duplicated or reordered")
		}
		end := day.Add(time.Duration(hour+1) * time.Hour).UTC()
		for _, field := range schema.fields {
			value, quality, err := parseValue(cells[field.col])
			if err != nil {
				return nil, fmt.Errorf("JMA %s at hour %d: %w", field.name, hour+1, err)
			}
			if value != nil {
				*value *= field.factor
			}
			start := end
			if field.aggregation != "instant" {
				start = end.Add(-time.Hour)
			}
			r := weather.Record{Source: "jma", Kind: "observation", LocationID: cfg.BlockNo, Variable: field.name, PeriodStart: start, PeriodEnd: end, Aggregation: field.aggregation, Value: value, Unit: field.unit, Quality: quality, RawValue: cells[field.col]}
			if cfg.isAMeDAS() {
				r.Details = map[string]string{"table_format": "hourly_a1"}
				// Since this date JMA publishes satellite-derived sunshine duration
				// for AMeDAS. Keep provenance separate from missing/quality markers.
				if field.name == "sunshine_duration" && !day.Before(amedasSunshineEstimateStart) {
					r.Kind = "estimate"
					r.Details["method"] = "jma_estimated_weather_distribution"
					if r.Quality == "observed" {
						r.Quality = "estimated"
					}
				}
			}
			records = append(records, r)
		}
		if schema.weatherCol < 0 {
			continue
		}
		raw := strings.TrimSpace(cells[schema.weatherCol])
		quality := "observed"
		value := raw
		if missing, ok := missingQuality(raw); ok {
			quality = missing
			value = ""
		}
		records = append(records, weather.Record{Source: "jma", Kind: "observation", LocationID: cfg.BlockNo, Variable: "weather_condition", PeriodStart: end, PeriodEnd: end, Aggregation: "instant", TextValue: value, Unit: "1", Quality: quality, RawValue: raw})
	}
	return records, nil
}

func missingQuality(raw string) (string, bool) {
	switch raw {
	case "":
		return "not_available", true
	case "×":
		return "missing", true
	case "///":
		return "not_observed_or_missing", true
	case "#":
		return "suspect", true
	}
	return "", false
}

func parseValue(raw string) (*float64, string, error) {
	raw = compact(raw)
	if quality, ok := missingQuality(raw); ok {
		return nil, quality, nil
	}
	if raw == "--" {
		return weather.Number(0), "no_phenomenon", nil
	}
	// Cloud cover bounds must not silently become exact zero or 100 percent.
	if raw == "0+" || raw == "10-" {
		return nil, "bounded", nil
	}
	quality := "observed"
	if strings.HasSuffix(raw, ")") {
		quality = "quasi_normal"
		raw = strings.TrimSuffix(raw, ")")
	}
	if strings.HasSuffix(raw, "]") {
		quality = "insufficient"
		raw = strings.TrimSuffix(raw, "]")
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return nil, "", errors.New("unrecognized numeric value or quality marker")
	}
	return weather.Number(v), quality, nil
}
