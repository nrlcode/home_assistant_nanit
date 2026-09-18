package notification

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"
)

const (
	sleepHistorySchema       = 1
	sleepHistoryMaxFile      = 1 << 20
	sleepHistoryMaxReports   = 60
	sleepHistoryMaxEvents    = 2000
	sleepHistoryMaxIntervals = 256
	sleepHistoryMaxDedupe    = 256
	sleepHistoryMaxTotal     = 64 << 20
)

type historyInterval struct {
	State    string `json:"state"`
	Start    string `json:"start"`
	End      string `json:"end,omitempty"`
	Obsolete bool   `json:"obsolete,omitempty"`
}

type historyReport struct {
	Key             string             `json:"key"`
	Status          string             `json:"status"`
	SourceUpdatedAt string             `json:"source_updated_at,omitempty"`
	WindowStart     string             `json:"window_start,omitempty"`
	WindowEnd       string             `json:"window_end,omitempty"`
	Metrics         map[string]float64 `json:"metrics,omitempty"`
	Intervals       []historyInterval  `json:"intervals,omitempty"`
	Digest          string             `json:"digest"`
	Revision        uint64             `json:"revision"`
}

type historyEvent struct {
	Key        string `json:"key"`
	Subtype    string `json:"subtype,omitempty"`
	OccurredAt string `json:"occurred_at"`
	UpdatedAt  string `json:"updated_at,omitempty"`
	DedupeHash string `json:"dedupe_hash"`
}

type historyFile struct {
	SchemaVersion  int             `json:"schema_version"`
	Revision       uint64          `json:"revision"`
	LastSuccessUTC string          `json:"last_success_utc,omitempty"`
	Reports        []historyReport `json:"reports,omitempty"`
	Events         []historyEvent  `json:"events,omitempty"`
	Watermark      string          `json:"watermark,omitempty"`
	TieHashes      []string        `json:"tie_hashes,omitempty"`
	HistoryStatus  string          `json:"history_status"`
}

type sleepHistory struct {
	dir   string
	files map[string]*historyFile
}

// reportForDate returns only the sanitized report for an explicit UTC date.
func (h *sleepHistory) reportForDate(scope, date string) (*historyReport, string) {
	f := h.files[scope]
	return reportForDate(f, date)
}

func reportForDate(f *historyFile, date string) (*historyReport, string) {
	if len(date) != len("2006-01-02") {
		return nil, "unavailable"
	}
	parsed, err := time.Parse("2006-01-02", date)
	if err != nil || parsed.After(time.Now().UTC().Truncate(24*time.Hour)) {
		return nil, "unavailable"
	}
	if f == nil {
		return nil, "unavailable"
	}
	prefix := date + "|"
	for i := range f.Reports {
		if len(f.Reports[i].Key) >= len(prefix) && f.Reports[i].Key[:len(prefix)] == prefix {
			r := f.Reports[i]
			return &r, "ok"
		}
	}
	return nil, "unavailable"
}

func newSleepHistory(dir string) *sleepHistory {
	return &sleepHistory{dir: dir, files: map[string]*historyFile{}}
}
func historyScope(babyUID, cameraUID string) string {
	sum := sha256.Sum256([]byte(babyUID + "\x00" + cameraUID))
	return hex.EncodeToString(sum[:])
}
func (h *sleepHistory) path(scope string) string { return filepath.Join(h.dir, scope+".json") }

func (h *sleepHistory) load(scope string, now time.Time) (*historyFile, error) {
	if h.dir == "" {
		return &historyFile{SchemaVersion: sleepHistorySchema, HistoryStatus: "memory_only"}, nil
	}
	if f := h.files[scope]; f != nil {
		return f, nil
	}
	if err := os.MkdirAll(h.dir, 0o700); err != nil {
		return nil, err
	}
	_ = os.Chmod(h.dir, 0o700)
	if err := h.enforceTotalBudget(scope); err != nil {
		return nil, err
	}
	path := h.path(scope)
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		f := &historyFile{SchemaVersion: sleepHistorySchema, HistoryStatus: "ok"}
		h.files[scope] = f
		return f, nil
	}
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("history file is not regular")
	}
	if info.Size() > sleepHistoryMaxFile {
		return h.discard(path, scope, "oversized")
	}
	fd, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer fd.Close()
	data, err := io.ReadAll(io.LimitReader(fd, sleepHistoryMaxFile+1))
	if err != nil {
		return nil, err
	}
	if len(data) > sleepHistoryMaxFile {
		return h.discard(path, scope, "oversized")
	}
	var f historyFile
	if json.Unmarshal(data, &f) != nil || f.SchemaVersion != sleepHistorySchema {
		return h.discard(path, scope, "invalid_schema")
	}
	h.prune(&f, now)
	h.files[scope] = &f
	return &f, nil
}
func (h *sleepHistory) discard(path, scope, reason string) (*historyFile, error) {
	_ = os.Remove(path)
	f := &historyFile{SchemaVersion: sleepHistorySchema, HistoryStatus: reason}
	h.files[scope] = f
	return f, nil
}
func (h *sleepHistory) prune(f *historyFile, now time.Time) {
	cut := now.UTC().Add(-30 * 24 * time.Hour)
	f.Events = filterHistoryEvents(f.Events, cut)
	if len(f.Events) > sleepHistoryMaxEvents {
		f.Events = f.Events[len(f.Events)-sleepHistoryMaxEvents:]
	}
	if len(f.Reports) > sleepHistoryMaxReports {
		f.Reports = f.Reports[len(f.Reports)-sleepHistoryMaxReports:]
	}
	if len(f.TieHashes) > sleepHistoryMaxDedupe {
		f.TieHashes = f.TieHashes[len(f.TieHashes)-sleepHistoryMaxDedupe:]
	}
}
func filterHistoryEvents(in []historyEvent, cut time.Time) []historyEvent {
	out := in[:0]
	for _, e := range in {
		if t, err := time.Parse(time.RFC3339Nano, e.OccurredAt); err == nil && !t.Before(cut) {
			out = append(out, e)
		}
	}
	return out
}

func (h *sleepHistory) save(scope string, f *historyFile) error {
	if h.dir == "" {
		h.files[scope] = f
		return nil
	}
	if err := os.MkdirAll(h.dir, 0o700); err != nil {
		return err
	}
	data, err := json.Marshal(f)
	if err != nil {
		return err
	}
	if len(data) > sleepHistoryMaxFile {
		return fmt.Errorf("history exceeds file limit")
	}
	tmp, err := os.CreateTemp(h.dir, ".sleep-*.tmp")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer os.Remove(name)
	if err = tmp.Chmod(0o600); err == nil {
		_, err = tmp.Write(data)
	}
	if err == nil {
		err = tmp.Sync()
	}
	cerr := tmp.Close()
	if err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(name, h.path(scope)); err != nil {
		return err
	}
	if err = h.enforceTotalBudget(scope); err != nil {
		return err
	}
	if d, e := os.Open(h.dir); e == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	h.files[scope] = f
	return nil
}

type historyDiskFile struct {
	path    string
	scope   string
	size    int64
	modTime time.Time
}

func (h *sleepHistory) enforceTotalBudget(currentScope string) error {
	entries, err := os.ReadDir(h.dir)
	if err != nil {
		return err
	}
	var total int64
	files := make([]historyDiskFile, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if len(name) != 69 || filepath.Ext(name) != ".json" {
			continue
		}
		scope := name[:64]
		if _, err := hex.DecodeString(scope); err != nil {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			continue
		}
		total += info.Size()
		files = append(files, historyDiskFile{path: filepath.Join(h.dir, name), scope: scope, size: info.Size(), modTime: info.ModTime()})
	}
	if total <= sleepHistoryMaxTotal {
		return nil
	}
	sort.Slice(files, func(i, j int) bool { return files[i].modTime.Before(files[j].modTime) })
	for _, file := range files {
		if total <= sleepHistoryMaxTotal {
			break
		}
		if file.scope == currentScope {
			continue
		}
		if err := os.Remove(file.path); err != nil {
			return err
		}
		delete(h.files, file.scope)
		total -= file.size
	}
	if total > sleepHistoryMaxTotal {
		return fmt.Errorf("history exceeds total limit")
	}
	return nil
}

func sanitizeReport(stats *SleepStats) historyReport {
	key := boundedLabel(stats.PeriodDate)
	if key == "" {
		key = boundedLabel(stats.Date)
	}
	key += "|" + boundedLabel(stats.PeriodPart) + "|" + boundedLabel(stats.Kind)
	r := historyReport{Key: key, Status: sleepReportStatus(stats), Metrics: map[string]float64{}}
	put := func(k string, v *int) {
		if v != nil && *v >= 0 {
			r.Metrics[k] = float64(*v)
		}
	}
	put("total_awake_time", stats.TotalAwakeTime)
	put("total_sleep_time", stats.TotalSleepTime)
	put("longest_sleep", stats.LongestSleep)
	put("times_woke_up", stats.TimesWokeUp)
	put("sleep_interventions", stats.SleepInterventions)
	put("times_out_of_crib", stats.TimesOutOfCrib)
	if stats.UpdatedAt != nil {
		r.SourceUpdatedAt = unixNumberRFC3339(stats.UpdatedAt)
	}
	if stats.Timerange != nil {
		r.WindowStart = unixNumberRFC3339(stats.Timerange.Start)
		r.WindowEnd = unixNumberRFC3339(stats.Timerange.End)
	}
	for _, s := range stats.States {
		if len(r.Intervals) >= sleepHistoryMaxIntervals {
			break
		}
		start := unixRFC3339(s.BeginTS)
		if start == "" {
			continue
		}
		end := unixRFC3339(s.EndTS)
		obsolete := s.Obsolete != nil && *s.Obsolete
		r.Intervals = append(r.Intervals, historyInterval{State: safeState(s.Title), Start: start, End: end, Obsolete: obsolete})
	}
	b, _ := json.Marshal(struct {
		Key, Status, Updated string
		Metrics              map[string]float64
		Intervals            []historyInterval
	}{r.Key, r.Status, r.SourceUpdatedAt, r.Metrics, r.Intervals})
	sum := sha256.Sum256(b)
	r.Digest = hex.EncodeToString(sum[:])
	return r
}
func boundedLabel(v *string) string {
	if v == nil || len(*v) > 64 {
		return ""
	}
	for _, r := range *v {
		if r < 0x20 || r == 0x7f {
			return ""
		}
	}
	return *v
}
func safeState(v string) string {
	switch v {
	case SleepStateAsleep, SleepStateAwake, SleepStateAbsent, SleepStateParentIntervention:
		return v
	}
	return SleepStateUnknown
}

func (h *sleepHistory) updateReport(scope string, stats *SleepStats, now time.Time) (*historyFile, error) {
	f, err := h.load(scope, now)
	if err != nil {
		return nil, err
	}
	r := sanitizeReport(stats)
	if r.Key == "||" {
		f.HistoryStatus = "report_key_missing"
		return f, h.save(scope, f)
	}
	replaced := false
	for i := range f.Reports {
		if f.Reports[i].Key == r.Key {
			if f.Reports[i].SourceUpdatedAt != "" && r.SourceUpdatedAt != "" && r.SourceUpdatedAt < f.Reports[i].SourceUpdatedAt {
				return f, nil
			}
			if f.Reports[i].Digest == r.Digest {
				return f, nil
			}
			r.Revision = f.Reports[i].Revision + 1
			f.Reports[i] = r
			replaced = true
			break
		}
	}
	if !replaced {
		r.Revision = 1
		f.Reports = append(f.Reports, r)
	}
	f.Revision++
	f.LastSuccessUTC = now.UTC().Format(time.RFC3339Nano)
	f.HistoryStatus = "ok"
	h.prune(f, now)
	return f, h.save(scope, f)
}
func (h *sleepHistory) updateEvents(scope string, events []SleepEvent, watermark time.Time, ties map[string]struct{}, now time.Time) (*historyFile, error) {
	f, err := h.load(scope, now)
	if err != nil {
		return nil, err
	}
	by := map[string]int{}
	for i, e := range f.Events {
		by[e.DedupeHash] = i
	}
	for _, e := range events {
		t, ok := e.Time()
		if !ok || !supportedSleepEvent(e) {
			continue
		}
		key := sleepEventPrivateKey(e)
		he := historyEvent{Key: e.Key, OccurredAt: t.Format(time.RFC3339Nano), DedupeHash: fmt.Sprintf("%x", sha256.Sum256([]byte(key)))}
		if e.Key == SleepEventKeyVisit && (e.InternalKey == "VISIT" || e.InternalKey == "VISIT_WOKE_UP" || e.InternalKey == "VISIT_FELL_ASLEEP") {
			he.Subtype = e.InternalKey
		}
		if e.UpdatedAt != nil {
			he.UpdatedAt = unixRFC3339(e.UpdatedAt)
		}
		if i, ok := by[he.DedupeHash]; ok {
			f.Events[i] = he
		} else {
			by[he.DedupeHash] = len(f.Events)
			f.Events = append(f.Events, he)
		}
	}
	sort.Slice(f.Events, func(i, j int) bool { return f.Events[i].OccurredAt < f.Events[j].OccurredAt })
	f.Watermark = watermark.UTC().Format(time.RFC3339Nano)
	f.TieHashes = f.TieHashes[:0]
	for k := range ties {
		f.TieHashes = append(f.TieHashes, fmt.Sprintf("%x", sha256.Sum256([]byte(k))))
	}
	sort.Strings(f.TieHashes)
	f.Revision++
	f.LastSuccessUTC = now.UTC().Format(time.RFC3339Nano)
	f.HistoryStatus = "ok"
	h.prune(f, now)
	return f, h.save(scope, f)
}
