package baby

import (
	"errors"
	"regexp"
)

var validUID = regexp.MustCompile(`^[a-z0-9_-]+$`)

// ErrInvalidBabyUID is returned when a baby UID contains unsafe characters
var ErrInvalidBabyUID = errors.New("baby UID contains unsafe characters")

// ValidateBabyUID - Checks that Baby UID does not contain any bad characters
// This is necessary because we use it as part of file paths
// Returns error if UID is invalid
func ValidateBabyUID(babyUID string) error {
	if !validUID.MatchString(babyUID) {
		return ErrInvalidBabyUID
	}
	return nil
}
