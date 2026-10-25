package weather

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func fixtureRecord() Record {
	t := time.Date(2026, 10, 25, 4, 0, 0, 0, time.UTC)
	return Record{Source: "jma", Kind: "observation", LocationID: "47638", Variable: "sunshine_duration", PeriodStart: t, PeriodEnd: t.Add(time.Hour), Aggregation: "sum", Value: Number(1800), Unit: "s", Quality: "observed"}
}
func openTestStore(t *testing.T, path string) *Store {
	t.Helper()
	s, err := OpenStore(path, "fixture")
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func TestJournalRestartRevisionAndReversion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	s := openTestStore(t, path)
	r := fixtureRecord()
	now := r.PeriodEnd.Add(24 * time.Hour)
	if err := s.Stage([]Record{r}, now, 120*24*time.Hour); err != nil {
		t.Fatal(err)
	}
	first := s.Pending()[0]
	if _, err := OpenStore(path, "fixture"); err == nil {
		t.Fatal("state lock was not enforced")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openTestStore(t, path)
	defer s.Close()
	if len(s.Pending()) != 1 || s.Pending()[0].EventID != first.EventID {
		t.Fatal("outbox was not restored")
	}
	if err := s.Ack(); err != nil {
		t.Fatal(err)
	}
	if err := s.Stage([]Record{r}, now.Add(time.Hour), 120*24*time.Hour); err != nil {
		t.Fatal(err)
	}
	if len(s.Pending()) != 0 {
		t.Fatal("unchanged observation re-emitted")
	}
	r.Value = Number(1200)
	if err := s.Stage([]Record{r}, now.Add(2*time.Hour), 120*24*time.Hour); err != nil {
		t.Fatal(err)
	}
	second := s.Pending()[0]
	if second.Revision != 2 || second.Supersedes != first.EventID {
		t.Fatal("revision chain broken")
	}
	if err := s.Ack(); err != nil {
		t.Fatal(err)
	}
	r = fixtureRecord()
	if err := s.Stage([]Record{r}, now.Add(3*time.Hour), 120*24*time.Hour); err != nil {
		t.Fatal(err)
	}
	third := s.Pending()[0]
	if third.EventID == first.EventID || third.Revision != 3 || third.Supersedes != second.EventID {
		t.Fatal("A -> B -> A lost a revision")
	}
	if err := s.Ack(); err != nil {
		t.Fatal(err)
	}
	r.Value = nil
	r.Quality = "missing"
	if err := s.Stage([]Record{r}, now.Add(4*time.Hour), 120*24*time.Hour); err != nil {
		t.Fatal(err)
	}
	if s.Pending()[0].Value != nil || s.Pending()[0].Revision != 4 {
		t.Fatal("withdrawal not represented")
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("state permissions", err)
	}
}

func TestStateFailClosedAndAtomicStage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	s := openTestStore(t, path)
	r := fixtureRecord()
	bad := r
	bad.Value = Number(999)
	if err := s.Stage([]Record{r, bad}, time.Now(), 24*time.Hour); err == nil {
		t.Fatal("conflicting duplicate accepted")
	}
	if len(s.data.Entries) != 0 || len(s.Pending()) != 0 {
		t.Fatal("failed stage changed state")
	}
	if err := s.SetCheckpoint("recent", r.PeriodEnd); err != nil {
		t.Fatal(err)
	}
	_ = s.Close()
	if _, err := OpenStore(path, "other location"); err == nil {
		t.Fatal("different namespace accepted")
	}
	s = openTestStore(t, path)
	if !s.Checkpoint("recent").Equal(r.PeriodEnd) {
		t.Fatal("checkpoint lost")
	}
	_ = s.Close()
	for _, body := range []string{"{broken", `{"version":2}`, `{"version":1,"namespace":"fixture","entries":{},"checkpoints":{}} {}`} {
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := OpenStore(path, "fixture"); err == nil {
			t.Fatal("corrupt state accepted")
		}
		got, _ := os.ReadFile(path)
		if string(got) != body {
			t.Fatal("corrupt state overwritten")
		}
	}
}

func TestSchemaKeepsNullAndUTCIntervals(t *testing.T) {
	r := fixtureRecord()
	r.Value = nil
	r.Quality = "missing"
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	var obj map[string]any
	if err = json.Unmarshal(b, &obj); err != nil {
		t.Fatal(err)
	}
	if v, ok := obj["value"]; !ok || v != nil {
		t.Fatal("null value must survive encoding")
	}
}
