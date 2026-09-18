package mqtt

import (
	"strings"
	"testing"

	"github.com/indiefan/home_assistant_nanit/pkg/baby"
)

func TestConnectionIsAuthorizedBaby(t *testing.T) {
	conn := &Connection{
		babies: make(map[string]string),
	}

	// Register a baby
	conn.RegisterBaby("baby123", "Test Baby")

	tests := []struct {
		name       string
		babyUID    string
		authorized bool
	}{
		{
			name:       "registered baby is authorized",
			babyUID:    "baby123",
			authorized: true,
		},
		{
			name:       "unregistered baby is not authorized",
			babyUID:    "unknown456",
			authorized: false,
		},
		{
			name:       "empty baby UID is not authorized",
			babyUID:    "",
			authorized: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := conn.IsAuthorizedBaby(tt.babyUID)
			if result != tt.authorized {
				t.Errorf("IsAuthorizedBaby(%q) = %v, want %v", tt.babyUID, result, tt.authorized)
			}
		})
	}
}

func TestConnectionRegisterBabyAddsToAuthorizedList(t *testing.T) {
	conn := &Connection{
		babies: make(map[string]string),
	}

	// Initially no babies are authorized
	if conn.IsAuthorizedBaby("baby1") {
		t.Error("Expected baby1 to be unauthorized before registration")
	}

	// Register the baby
	conn.RegisterBaby("baby1", "Baby One")

	// Now should be authorized
	if !conn.IsAuthorizedBaby("baby1") {
		t.Error("Expected baby1 to be authorized after registration")
	}
}

func TestConnectionRegisterBabiesFromList(t *testing.T) {
	conn := &Connection{
		babies: make(map[string]string),
	}

	babies := []baby.Baby{
		{UID: "baby1", Name: "Baby One"},
		{UID: "baby2", Name: "Baby Two"},
		{UID: "baby3", Name: "Baby Three"},
	}

	conn.RegisterBabies(babies)

	// All babies should be authorized
	for _, b := range babies {
		if !conn.IsAuthorizedBaby(b.UID) {
			t.Errorf("Expected %s to be authorized after registration", b.UID)
		}
	}

	// Unregistered baby should not be authorized
	if conn.IsAuthorizedBaby("unknown") {
		t.Error("Expected unknown baby to be unauthorized")
	}
}

func TestValidateBabyCommandAuth(t *testing.T) {
	conn := &Connection{
		babies: make(map[string]string),
	}

	// Register a known baby
	conn.RegisterBaby("valid-baby-123", "Valid Baby")

	tests := []struct {
		name        string
		babyUID     string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "valid and authorized baby",
			babyUID:     "valid-baby-123",
			expectError: false,
		},
		{
			name:        "valid format but unauthorized baby",
			babyUID:     "unauthorized-baby",
			expectError: true,
			errorMsg:    "baby UID not authorized",
		},
		{
			name:        "empty baby UID",
			babyUID:     "",
			expectError: true,
			errorMsg:    "baby UID cannot be empty",
		},
		{
			name:        "baby UID with invalid characters",
			babyUID:     "<script>alert('xss')</script>",
			expectError: true,
			errorMsg:    "invalid baby UID format",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := conn.ValidateBabyCommandAuth(tt.babyUID)
			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error containing %q, got nil", tt.errorMsg)
				} else if tt.errorMsg != "" && !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("Expected error containing %q, got %q", tt.errorMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error, got %v", err)
				}
			}
		})
	}
}
