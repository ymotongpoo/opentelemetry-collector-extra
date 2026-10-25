package jmareceiver

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ymotongpoo/opentelemetry-collector-extra/internal/weather"
)

func testConfig(t *testing.T) *Config {
	t.Helper()
	c := createDefaultConfig().(*Config)
	c.PrecNo = "49"
	c.BlockNo = "47638"
	c.StationName = "甲府"
	c.StateFile = filepath.Join(t.TempDir(), "state.json")
	return c
}
func fixture(t *testing.T) []byte {
	t.Helper()
	b, e := os.ReadFile("testdata/kofu-hourly.html")
	if e != nil {
		t.Fatal(e)
	}
	return b
}
func fixtureDay() time.Time { return time.Date(2026, 10, 24, 0, 0, 0, 0, japan) }

func TestJMAIntervalsUnitsAndUnavailableValues(t *testing.T) {
	rs, err := parsePage(fixture(t), fixtureDay(), testConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(rs) != 144 {
		t.Fatal("record count", len(rs))
	}
	find := func(name string, h int) weather.Record {
		for _, r := range rs {
			if r.Variable == name && r.PeriodEnd.Equal(fixtureDay().Add(time.Duration(h)*time.Hour)) {
				return r
			}
		}
		t.Fatal("not found", name, h)
		return weather.Record{}
	}
	r := find("shortwave_radiation", 7)
	if r.Value == nil || *r.Value < 8.3333 || *r.Value > 8.3334 || r.PeriodStart != fixtureDay().Add(6*time.Hour).UTC() {
		t.Fatal("MJ/m2 conversion or preceding hour", r)
	}
	r = find("sunshine_duration", 1)
	if r.Value != nil || r.Quality != "not_available" {
		t.Fatal("blank became night-time zero")
	}
	r = find("precipitation", 1)
	if r.Value == nil || *r.Value != 0 || r.Quality != "no_phenomenon" {
		t.Fatal("no precipitation marker", r)
	}
	r = find("weather_condition", 1)
	if r.TextValue != "曇" || r.PeriodStart != r.PeriodEnd {
		t.Fatal("weather image alt or instant time", r)
	}
	if !find("air_temperature", 24).PeriodEnd.Equal(fixtureDay().AddDate(0, 0, 1)) {
		t.Fatal("24:00 not next day")
	}
}

func TestQualityMarkers(t *testing.T) {
	for _, tc := range []struct {
		raw, quality string
		nilValue     bool
	}{
		{"1.2 )", "quasi_normal", false}, {"0.0 ]", "insufficient", false}, {"×", "missing", true}, {"///", "not_observed_or_missing", true}, {"#", "suspect", true}, {"0+", "bounded", true}, {"10-", "bounded", true}, {"0.0", "observed", false}, {"--", "no_phenomenon", false},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			v, q, e := parseValue(tc.raw)
			if e != nil || q != tc.quality || (v == nil) != tc.nilValue {
				t.Fatal(v, q, e)
			}
		})
	}
	if _, _, err := parseValue("unrecognized"); err == nil {
		t.Fatal("unknown quality silently ignored")
	}
}

func TestRejectUnpublishedWrongDateAndChangedSchema(t *testing.T) {
	c := testConfig(t)
	data := string(fixture(t))
	for _, body := range []string{"<html>not published</html>", strings.Replace(data, "2026年10月24日", "2026年10月23日", 1), strings.Replace(data, "甲府（", "東京（", 1), strings.Replace(data, "MJ/㎡", "kWh/㎡", 1), strings.Replace(data, ">24</td>", ">23</td>", 1)} {
		if _, err := parsePage([]byte(body), fixtureDay(), c); err == nil {
			t.Fatal("bad page accepted")
		}
	}
}

func TestDailyWeeklyBackfillAndResume(t *testing.T) {
	base := string(fixture(t))
	calls := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		calls = append(calls, q.Get("day"))
		if q.Get("prec_no") != "49" || q.Get("block_no") != "47638" {
			t.Error("wrong station")
		}
		date := fmt.Sprintf("%s年%s月%s日", q.Get("year"), q.Get("month"), q.Get("day"))
		_, _ = w.Write([]byte(strings.Replace(base, "2026年10月24日", date, 1)))
	}))
	defer server.Close()
	c := testConfig(t)
	c.Endpoint = server.URL
	c.RecentDays = 2
	c.RevisionDays = 4
	c.MinRequestInterval = 0
	c.Backfill = BackfillConfig{StartDate: "2026-10-01", EndDate: "2026-10-02", RequestID: "initial"}
	s := newSource(c)
	store, err := weather.OpenStore(c.StateFile, "fixture")
	if err != nil {
		t.Fatal(err)
	}
	emitted := 0
	publish := func(rs []weather.Record, at time.Time) error {
		if e := store.Stage(rs, at, c.StateTTL); e != nil {
			return e
		}
		emitted += len(store.Pending())
		return store.Ack()
	}
	now := fixtureDay().Add(36 * time.Hour)
	if err = s.collect(context.Background(), now, store, publish); err != nil {
		t.Fatal(err)
	}
	if strings.Join(calls, ",") != "24,23,22,21,1,2" || emitted != 6*144 {
		t.Fatal(calls, emitted)
	}
	_ = store.Close()
	store, err = weather.OpenStore(c.StateFile, "fixture")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err = s.collect(context.Background(), now.Add(time.Hour), store, publish); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 6 {
		t.Fatal("restart repeated completed retrieval")
	}
	if err = s.collect(context.Background(), now.Add(24*time.Hour), store, publish); err != nil {
		t.Fatal(err)
	}
	if strings.Join(calls[6:], ",") != "25,24" {
		t.Fatal("daily schedule", calls)
	}
	if err = s.collect(context.Background(), now.Add(7*24*time.Hour), store, publish); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 12 {
		t.Fatal("weekly schedule", calls)
	}
}

func TestFailedPassDoesNotAdvanceCheckpoint(t *testing.T) {
	data := fixture(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(data) }))
	defer server.Close()
	c := testConfig(t)
	c.Endpoint = server.URL
	c.RecentDays = 1
	c.RevisionDays = 1
	c.MinRequestInterval = 0
	store, err := weather.OpenStore(c.StateFile, "fixture")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	s := newSource(c)
	reject := func([]weather.Record, time.Time) error { return errors.New("consumer failed") }
	if err = s.collect(context.Background(), fixtureDay().Add(36*time.Hour), store, reject); err == nil {
		t.Fatal("failure swallowed")
	}
	if !store.Checkpoint("recent").IsZero() {
		t.Fatal("failed pass checkpointed")
	}
}

func TestConfig(t *testing.T) {
	c := testConfig(t)
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*Config){"state": func(c *Config) { c.StateFile = "" }, "station": func(c *Config) { c.BlockNo = "abc" }, "revision range": func(c *Config) { c.RevisionDays = 1 }, "interval": func(c *Config) { c.RevisionInterval = time.Hour }, "backfill": func(c *Config) { c.Backfill = BackfillConfig{StartDate: "2026-10-01"} }} {
		t.Run(name, func(t *testing.T) {
			c := testConfig(t)
			change(c)
			if c.Validate() == nil {
				t.Fatal("bad config accepted")
			}
		})
	}
}
