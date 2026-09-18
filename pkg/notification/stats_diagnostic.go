package notification

import (
	"bytes"
	"encoding/json"
	"regexp"
)

var diagnosticKeys = []string{"valid", "date", "bed_start_time", "num_events", "longest_sleep", "times_out_of_crib", "ongoing", "sleep_end_time", "sleep_start_time", "sleep_onset", "total_present_time", "kind", "last_wake_up", "sleep_sessions", "parent_interventions", "soothing_events"}
var diagnosticNumber = regexp.MustCompile(`^-?[0-9]+(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?$`)
var diagnosticDate = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}$`)

type DiagnosticField struct {
	Presence  string `json:"presence"`
	Semantics string `json:"semantics"`
	Type      string `json:"type,omitempty"`
	Value     string `json:"value,omitempty"`
	Redacted  bool   `json:"redacted"`
}

func ProjectStatsDiagnostic(raw []byte) (map[string]DiagnosticField, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var values map[string]json.RawMessage
	if err := decoder.Decode(&values); err != nil {
		return nil, err
	}
	out := make(map[string]DiagnosticField, len(diagnosticKeys))
	for _, key := range diagnosticKeys {
		field := DiagnosticField{Presence: "absent", Semantics: "unverified"}
		value, ok := values[key]
		if !ok {
			out[key] = field
			continue
		}
		field.Presence = "value"
		trimmed := bytes.TrimSpace(value)
		if bytes.Equal(trimmed, []byte("null")) {
			field.Presence = "null"
			out[key] = field
			continue
		}
		switch {
		case bytes.Equal(trimmed, []byte("true")) || bytes.Equal(trimmed, []byte("false")):
			field.Type = "bool"
			field.Value = string(trimmed)
		case len(trimmed) <= 64 && diagnosticNumber.Match(trimmed):
			field.Type = "number"
			field.Value = string(trimmed)
		default:
			var text string
			if json.Unmarshal(trimmed, &text) == nil && len(text) <= 128 && ((key == "date" && diagnosticDate.MatchString(text)) || (key == "kind" && (text == "night" || text == "nap"))) {
				field.Type = "string"
				field.Value = text
			} else {
				field.Redacted = true
			}
		}
		out[key] = field
	}
	return out, nil
}
