package notification

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSleepHistoryContract(t *testing.T) {
	dir := t.TempDir()
	h := newSleepHistory(dir)
	scope := historyScope("private-baby", "private-camera")
	period, part, kind := "2026-09-18", "night", "sleep"
	valid, sealed, ongoing := true, true, false
	begin, end := 1789700000.25, 1789703600.75
	stats := &SleepStats{PeriodDate: &period, PeriodPart: &part, Kind: &kind, Valid: &valid, Sealed: &sealed, Ongoing: &ongoing, States: []SleepState{{Title: SleepStateAsleep, BeginTS: &begin, EndTS: &end, UID: "identity-canary", BabyUID: "baby-canary"}}}
	f, err := h.updateReport(scope, stats, time.Unix(1789704000, 0))
	if err != nil {
		t.Fatal(err)
	}
	if len(f.Reports) != 1 || len(f.Reports[0].Intervals) != 1 {
		t.Fatalf("history=%#v", f)
	}
	path := filepath.Join(dir, scope+".json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, canary := range []string{"private-baby", "private-camera", "identity-canary", "baby-canary", "https://"} {
		if strings.Contains(string(data), canary) {
			t.Fatalf("private canary persisted: %q", canary)
		}
	}
	if info, _ := os.Stat(path); info.Mode().Perm() != 0o600 {
		t.Fatalf("mode=%o", info.Mode().Perm())
	}
	var decoded historyFile
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	stats.States = nil
	if _, err := h.updateReport(scope, stats, time.Unix(1789705000, 0)); err != nil {
		t.Fatal(err)
	}
	if got := len(h.files[scope].Reports[0].Intervals); got != 0 {
		t.Fatalf("stale intervals retained: %d", got)
	}
}

func TestSleepHistoryRejectsSymlinkAndUnknownSchema(t *testing.T) {
	dir := t.TempDir()
	h := newSleepHistory(dir)
	scope := historyScope("b", "c")
	target := filepath.Join(dir, "target")
	if err := os.WriteFile(target, []byte(`{"schema_version":1}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, h.path(scope)); err != nil {
		t.Fatal(err)
	}
	if _, err := h.load(scope, time.Now()); err == nil {
		t.Fatal("symlink accepted")
	}
	_ = os.Remove(h.path(scope))
	if err := os.WriteFile(h.path(scope), []byte(`{"schema_version":99}`), 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := newSleepHistory(dir).load(scope, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if f.HistoryStatus != "invalid_schema" {
		t.Fatalf("status=%q", f.HistoryStatus)
	}
}

func TestSleepHistoryEnforcesTotalBudgetAcrossCameras(t *testing.T) {
	dir := t.TempDir()
	oldest := ""
	for i := 0; i < 17; i++ {
		scope := fmt.Sprintf("%064x", i+1)
		path := filepath.Join(dir, scope+".json")
		if err := os.WriteFile(path, make([]byte, sleepHistoryMaxFile-1), 0o600); err != nil {
			t.Fatal(err)
		}
		when := time.Unix(int64(i+1), 0)
		if err := os.Chtimes(path, when, when); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			oldest = path
		}
	}
	unrelated := filepath.Join(dir, "keep.txt")
	if err := os.WriteFile(unrelated, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	h := newSleepHistory(dir)
	if err := h.save(strings.Repeat("f", 64), &historyFile{SchemaVersion: sleepHistorySchema, HistoryStatus: "ok"}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(oldest); !os.IsNotExist(err) {
		t.Fatalf("oldest history was not pruned: %v", err)
	}
	if _, err := os.Stat(unrelated); err != nil {
		t.Fatalf("unrelated file changed: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".json") {
			info, err := entry.Info()
			if err != nil {
				t.Fatal(err)
			}
			total += info.Size()
		}
	}
	if total > sleepHistoryMaxTotal {
		t.Fatalf("history total=%d exceeds %d", total, sleepHistoryMaxTotal)
	}
}
