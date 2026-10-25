package jmareceiver

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ymotongpoo/opentelemetry-collector-extra/internal/weather"
)

func amedasConfig(t *testing.T) *Config {
	t.Helper()
	c := testConfig(t)
	c.BlockNo, c.StationName = "0433", "勝沼"
	return c
}

func amedasFixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestAutomaticTableSelectionAndRequest(t *testing.T) {
	for _, tc := range []struct{ block, page string }{{"0433", "hourly_a1.php"}, {"1165", "hourly_a1.php"}, {"47638", "hourly_s1.php"}, {"47662", "hourly_s1.php"}} {
		t.Run(tc.block, func(t *testing.T) {
			c := testConfig(t)
			c.BlockNo = tc.block
			if err := c.Validate(); err != nil {
				t.Fatal(err)
			}
			u, err := url.Parse(newSource(c).requestURL(fixtureDay()))
			if err != nil || u.Path != "/stats/etrn/view/"+tc.page || u.Query().Get("block_no") != tc.block || u.Query().Get("prec_no") != "49" {
				t.Fatal("wrong table or station", u, err)
			}
			c.Endpoint = "https://example.com/custom-hourly"
			if u, _ = url.Parse(newSource(c).requestURL(fixtureDay())); u.Host != "example.com" || u.Path != "/custom-hourly" {
				t.Fatal("explicit endpoint was ignored", u)
			}
		})
	}
	// Preserve the effective URL used in state identity before auto-selection.
	c := testConfig(t)
	if c.endpoint() != "https://www.data.jma.go.jp/stats/etrn/view/hourly_s1.php" {
		t.Fatal("existing weather-station state identity changed")
	}
	for _, id := range []string{"433", "00433x", "123456", "", "０４３３"} {
		c.BlockNo = id
		if c.Validate() == nil {
			t.Fatal("invalid station accepted", id)
		}
	}
}

func TestAMeDASIntervalsQualityAndCoverage(t *testing.T) {
	c := amedasConfig(t)
	rs, err := parsePage(amedasFixture(t, "katsunuma-hourly.html"), fixtureDay(), c)
	if err != nil {
		t.Fatal(err)
	}
	if len(rs) != 72 {
		t.Fatal("expected three fields for each hour", len(rs))
	}
	for _, r := range rs {
		if r.LocationID != "0433" || r.Details["table_format"] != "hourly_a1" {
			t.Fatal("lost leading zero or provenance", r)
		}
		switch r.Variable {
		case "precipitation":
			if r.Kind != "observation" || r.Unit != "mm" || r.PeriodEnd.Sub(r.PeriodStart) != time.Hour {
				t.Fatal("precipitation semantics", r)
			}
		case "air_temperature":
			if r.Kind != "observation" || r.Unit != "Cel" || !r.PeriodStart.Equal(r.PeriodEnd) {
				t.Fatal("temperature semantics", r)
			}
		case "sunshine_duration":
			if r.Kind != "estimate" || r.Details["method"] != "jma_estimated_weather_distribution" || r.Unit != "s" || r.PeriodEnd.Sub(r.PeriodStart) != time.Hour {
				t.Fatal("estimated sunshine semantics", r)
			}
			if r.Value == nil && r.Quality != "not_available" || r.Value != nil && r.Quality != "estimated" {
				t.Fatal("blank/estimate quality", r)
			}
		default:
			t.Fatal("fabricated unavailable field", r.Variable)
		}
	}
	if rs[0].Value == nil || *rs[0].Value != 0 || rs[1].Value == nil || *rs[1].Value != 16.2 || rs[2].Value != nil {
		t.Fatal("columns shifted or blank sunshine became zero")
	}
	if !rs[len(rs)-1].PeriodEnd.Equal(fixtureDay().AddDate(0, 0, 1)) {
		t.Fatal("24:00 shifted")
	}
	// Rain-only stations use the same table, with unavailable fields marked ///.
	c.BlockNo, c.StationName = "1165", "上野原"
	rs, err = parsePage(amedasFixture(t, "uenohara-hourly.html"), fixtureDay(), c)
	if err != nil || len(rs) != 72 {
		t.Fatal(len(rs), err)
	}
	for _, r := range rs {
		if r.Variable != "precipitation" && (r.Value != nil || r.Quality != "not_observed_or_missing") {
			t.Fatal("unobserved value fabricated", r)
		}
	}
}

func TestAMeDASSunshineTransitionAndConversion(t *testing.T) {
	c := amedasConfig(t)
	old := amedasFixture(t, "katsunuma-2020-hourly.html")
	day := time.Date(2020, 10, 24, 0, 0, 0, 0, japan)
	rs, err := parsePage(old, day, c)
	if err != nil {
		t.Fatal(err)
	}
	// 08:00's 0.3 hours must be 1080 seconds, not an instantaneous reading.
	r := rs[7*3+2]
	if r.Kind != "observation" || r.Quality != "observed" || r.Value == nil || *r.Value != 1080 || r.PeriodStart != day.Add(7*time.Hour).UTC() {
		t.Fatal("historical observed sunshine", r)
	}
	for _, date := range []string{"2021年3月1日", "2021年3月2日"} {
		day := amedasSunshineEstimateStart
		want := "estimate"
		if strings.Contains(date, "1日") {
			day, want = day.AddDate(0, 0, -1), "observation"
		}
		data := strings.Replace(string(old), "2020年10月24日", date, 1)
		rs, err := parsePage([]byte(data), day, c)
		if err != nil || rs[7*3+2].Kind != want {
			t.Fatal("provenance transition", date, err)
		}
	}
}

func TestAMeDASRejectsSchemaAndIdentityDrift(t *testing.T) {
	c := amedasConfig(t)
	base := string(amedasFixture(t, "katsunuma-hourly.html"))
	for name, body := range map[string]string{
		"wrong format": string(fixture(t)),
		"date":         strings.Replace(base, "2026年10月24日", "2026年10月23日", 1),
		"station":      strings.Replace(base, "勝沼（", "大月（", 1),
		"column":       strings.Replace(base, "日照<br>時間<br>(h)", "日照<br>時間<br>(min)", 1),
		"subheader":    strings.Replace(base, "平均風速", "最大風速", 1),
		"span":         strings.Replace(base, `colspan="2"`, `colspan="3"`, 1),
		"hour":         strings.Replace(base, ">24</td>", ">23</td>", 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parsePage([]byte(body), fixtureDay(), c); err == nil {
				t.Fatal("invalid table accepted")
			}
		})
	}
	if _, err := parsePage([]byte(base), fixtureDay(), testConfig(t)); err == nil {
		t.Fatal("AMeDAS table accepted as meteorological table")
	}
}

func TestAMeDASRecheckDeduplicationAndWithdrawal(t *testing.T) {
	data := amedasFixture(t, "katsunuma-hourly.html")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("block_no") != "0433" {
			t.Error("station ID lost its leading zero")
		}
		_, _ = w.Write(data)
	}))
	defer server.Close()
	c := amedasConfig(t)
	c.Endpoint, c.MinRequestInterval = server.URL, 0
	store, err := weather.OpenStore(c.StateFile, "amedas-test")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	s := newSource(c)
	var last []weather.Event
	publish := func(rs []weather.Record, at time.Time) error {
		if err := store.Stage(rs, at, c.StateTTL); err != nil {
			return err
		}
		last = append([]weather.Event(nil), store.Pending()...)
		return store.Ack()
	}
	if err = s.fetchDay(context.Background(), fixtureDay(), publish); err != nil || len(last) != 72 {
		t.Fatal(len(last), err)
	}
	oldID := last[1].EventID
	if err = s.fetchDay(context.Background(), fixtureDay(), publish); err != nil || len(last) != 0 {
		t.Fatal("unchanged records repeated", len(last), err)
	}
	data = []byte(strings.Replace(string(data), ">16.2</td>", ">×</td>", 1))
	if err = s.fetchDay(context.Background(), fixtureDay(), publish); err != nil || len(last) != 1 {
		t.Fatal("correction not emitted", len(last), err)
	}
	if last[0].Value != nil || last[0].Quality != "missing" || last[0].Revision != 2 || last[0].Supersedes != oldID {
		t.Fatal("withdrawal chain", last)
	}
}
