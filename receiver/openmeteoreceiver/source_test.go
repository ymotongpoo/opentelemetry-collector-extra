package openmeteoreceiver

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ymotongpoo/opentelemetry-collector-extra/internal/weather"
)

func testConfig(t *testing.T) *Config {
	t.Helper()
	c := createDefaultConfig().(*Config)
	c.Latitude = weather.Number(35.6667)
	c.Longitude = weather.Number(138.55)
	c.LocationID = "kofu"
	c.StateFile = filepath.Join(t.TempDir(), "state.json")
	return c
}
func fixture(t *testing.T) []byte {
	t.Helper()
	b, e := os.ReadFile("testdata/forecast.json")
	if e != nil {
		t.Fatal(e)
	}
	return b
}

func TestParseForecastIntervalsAndKinds(t *testing.T) {
	data := fixture(t)
	c := testConfig(t)
	rs, err := parseResponse(data, c)
	if err != nil {
		t.Fatal(err)
	}
	if len(rs) != 22 {
		t.Fatalf("records=%d", len(rs))
	}
	var raw response
	if err = json.Unmarshal(data, &raw); err != nil {
		t.Fatal(err)
	}
	var stamp int64
	_ = json.Unmarshal(raw.Hourly["time"][0], &stamp)
	for _, r := range rs {
		if r.Variable == "shortwave_radiation" && r.PeriodEnd.Unix() == stamp {
			if r.PeriodStart.Unix() != stamp-3600 || r.Aggregation != "mean" || r.Unit != "W/m2" {
				t.Fatal("preceding hour was shifted", r)
			}
		}
		if r.Variable == "weather_code" && r.PeriodStart != r.PeriodEnd {
			t.Fatal("instant weather code treated as hourly sum")
		}
		if r.Variable == "sunrise" && (r.Kind != "astronomical" || r.Aggregation != "event_time" || r.PeriodEnd.Sub(r.PeriodStart) != 24*time.Hour) {
			t.Fatal("sunrise semantics", r)
		}
	}
}

func TestMissingValueAndMalformedResponses(t *testing.T) {
	c := testConfig(t)
	var raw response
	_ = json.Unmarshal(fixture(t), &raw)
	raw.Hourly["shortwave_radiation"][0] = json.RawMessage("null")
	b, _ := json.Marshal(raw)
	rs, err := parseResponse(b, c)
	if err != nil {
		t.Fatal(err)
	}
	if rs[0].Value != nil || rs[0].Quality != "missing" {
		t.Fatal("missing value became zero")
	}
	raw.HourlyUnits["shortwave_radiation"] = "MJ/m²"
	b, _ = json.Marshal(raw)
	if _, err = parseResponse(b, c); err == nil {
		t.Fatal("unit drift accepted")
	}
	_ = json.Unmarshal(fixture(t), &raw)
	raw.Hourly["weather_code"] = raw.Hourly["weather_code"][:1]
	b, _ = json.Marshal(raw)
	if _, err = parseResponse(b, c); err == nil {
		t.Fatal("truncated response accepted")
	}
	_ = json.Unmarshal(fixture(t), &raw)
	raw.Hourly["time"][1] = raw.Hourly["time"][0]
	b, _ = json.Marshal(raw)
	if _, err = parseResponse(b, c); err == nil {
		t.Fatal("duplicate time accepted")
	}
	for name, mutate := range map[string]func(*response){
		"missing grid": func(r *response) { r.Latitude = nil },
		"time units":   func(r *response) { r.DailyUnits["time"] = "milliseconds" },
	} {
		t.Run(name, func(t *testing.T) {
			var raw response
			_ = json.Unmarshal(fixture(t), &raw)
			mutate(&raw)
			b, _ := json.Marshal(raw)
			if _, err := parseResponse(b, c); err == nil {
				t.Fatal("invalid response accepted")
			}
		})
	}
}

func TestRequestAndChangeOnlyPublishing(t *testing.T) {
	data := fixture(t)
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		q := r.URL.Query()
		if q.Get("timeformat") != "unixtime" || q.Get("timezone") != "Asia/Tokyo" || q.Get("latitude") != "35.6667" || q.Get("past_days") != "2" {
			t.Error("unexpected request", q)
		}
		_, _ = w.Write(data)
	}))
	defer server.Close()
	c := testConfig(t)
	c.Endpoint = server.URL
	s := newSource(c)
	store, err := weather.OpenStore(c.StateFile, "fixture")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	counts := []int{}
	publish := func(rs []weather.Record, at time.Time) error {
		if err := store.Stage(rs, at, c.StateTTL); err != nil {
			return err
		}
		counts = append(counts, len(store.Pending()))
		return store.Ack()
	}
	for range 2 {
		if err = s.collect(context.Background(), time.Now(), store, publish); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 2 || len(counts) != 2 || counts[0] != 22 || counts[1] != 0 {
		t.Fatal(calls, counts)
	}
}

func TestConfig(t *testing.T) {
	c := testConfig(t)
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*Config){"latitude": func(c *Config) { c.Latitude = nil }, "state": func(c *Config) { c.StateFile = "" }, "coordinate": func(c *Config) { c.Longitude = weather.Number(181) }, "interval": func(c *Config) { c.CollectionInterval = 0 }, "timezone": func(c *Config) { c.Timezone = "nonsense" }, "multiple models": func(c *Config) { c.Model = "a,b" }, "past days": func(c *Config) { c.PastDays = 93 }, "credentials": func(c *Config) { c.Endpoint = "https://secret@example.com" }} {
		t.Run(name, func(t *testing.T) {
			c := testConfig(t)
			change(c)
			if c.Validate() == nil {
				t.Fatal("invalid config accepted")
			}
		})
	}
}
