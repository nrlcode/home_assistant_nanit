package notification

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestProjectStatsDiagnosticPresenceRedactionAndAllowlist(t *testing.T) {
	raw := []byte(`{"valid":true,"date":"2026-09-17","bed_start_time":0,"ongoing":false,"kind":null,"last_wake_up":"https://secret.example/token","unknown":"secret-sentinel","states":[{"baby_uid":"private"}]}`)
	fields, err := ProjectStatsDiagnostic(raw)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"valid", "date", "bed_start_time", "ongoing", "kind", "last_wake_up"} {
		if _, ok := fields[key]; !ok {
			t.Errorf("missing %s", key)
		}
	}
	if fields["kind"].Presence != "null" || fields["bed_start_time"].Value != "0" || fields["ongoing"].Value != "false" {
		t.Fatalf("presence/value %#v", fields)
	}
	if !fields["last_wake_up"].Redacted || fields["last_wake_up"].Value != "" {
		t.Fatalf("unsafe string %#v", fields["last_wake_up"])
	}
	encoded, _ := json.Marshal(fields)
	if strings.Contains(string(encoded), "secret-sentinel") || strings.Contains(string(encoded), "private") {
		t.Fatalf("private value escaped: %s", encoded)
	}
}

func TestProjectStatsDiagnosticMarksAbsent(t *testing.T) {
	fields, err := ProjectStatsDiagnostic([]byte(`{"valid":true}`))
	if err != nil {
		t.Fatal(err)
	}
	if fields["date"].Presence != "absent" {
		t.Fatalf("date=%#v", fields["date"])
	}
}
