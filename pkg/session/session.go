package session

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/indiefan/home_assistant_nanit/pkg/baby"
	"github.com/rs/zerolog/log"
)

// Revision - marks the version of the structure of a session file. Only files with equal revision will be loaded
// Note: you should increment this whenever you change the Session structure
const Revision = 3

// Session - application session data container
type Session struct {
	Revision            int         `json:"revision"`
	AuthToken           string      `json:"authToken"`
	AuthTime            time.Time   `json:"authTime"`
	Babies              []baby.Baby `json:"babies"`
	RefreshToken        string      `json:"refreshToken"`
	LastSeenMessageTime time.Time   `json:"lastSeenMessageTime"`
}

// Store - application session store context
type Store struct {
	mu            sync.RWMutex
	saveMu        sync.Mutex // serializes Save so concurrent snapshots cannot overwrite newer ones
	afterSnapshot func()
	Filename      string
	Session       *Session
}

// GetAuthToken returns the current auth token in a thread-safe manner
func (s *Store) GetAuthToken() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Session.AuthToken
}

// GetRefreshToken returns the current refresh token in a thread-safe manner
func (s *Store) GetRefreshToken() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Session.RefreshToken
}

// UpdateAuth atomically updates both tokens and the auth timestamp
func (s *Store) UpdateAuth(authToken, refreshToken string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Session.AuthToken = authToken
	s.Session.RefreshToken = refreshToken
	s.Session.AuthTime = time.Now()
}

// GetAuthTime returns the last auth timestamp in a thread-safe manner
func (s *Store) GetAuthTime() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Session.AuthTime
}

// GetBabies returns a copy of the known babies in a thread-safe manner
func (s *Store) GetBabies() []baby.Baby {
	s.mu.RLock()
	defer s.mu.RUnlock()
	babies := make([]baby.Baby, len(s.Session.Babies))
	copy(babies, s.Session.Babies)
	return babies
}

// UpdateBabies atomically replaces the known babies
func (s *Store) UpdateBabies(babies []baby.Baby) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Session.Babies = babies
}

// EnsureRefreshToken sets the refresh token from fallback only when empty
func (s *Store) EnsureRefreshToken(fallback string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Session.RefreshToken == "" {
		s.Session.RefreshToken = fallback
	}
}

// NewSessionStore - constructor
func NewSessionStore() *Store {
	return &Store{
		Session: &Session{Revision: Revision},
	}
}

// Load - loads previous state from a file
// Returns error if file exists but cannot be read/parsed
func (store *Store) Load() error {
	if _, err := os.Stat(store.Filename); os.IsNotExist(err) {
		log.Info().Str("filename", store.Filename).Msg("No app session file found")
		return nil
	}

	f, err := os.Open(store.Filename)
	if err != nil {
		log.Error().Str("filename", store.Filename).Err(err).Msg("Unable to open app session file")
		return fmt.Errorf("unable to open session file: %w", err)
	}
	defer f.Close()

	session := &Session{}
	if err := json.NewDecoder(f).Decode(session); err != nil {
		log.Error().Str("filename", store.Filename).Err(err).Msg("Unable to decode app session file")
		return fmt.Errorf("unable to decode session file: %w", err)
	}

	if session.Revision == Revision {
		store.mu.Lock()
		store.Session = session
		store.mu.Unlock()
		log.Info().Str("filename", store.Filename).Msg("Loaded app session from the file")
	} else {
		log.Warn().Str("filename", store.Filename).Msg("App session file contains older revision of the state, ignoring")
	}

	return nil
}

// Save - stores current data in a file using atomic write
// Serializes snapshot and write under saveMu so a stale concurrent
// snapshot cannot overwrite newer state. The fallback
// copy path is not crash-atomic; callers must treat it as best-effort.
// Returns error if save fails
func (store *Store) Save() error {
	if store.Filename == "" {
		return nil
	}

	log.Trace().Str("filename", store.Filename).Msg("Storing app session to the file")

	store.saveMu.Lock()
	defer store.saveMu.Unlock()

	store.mu.RLock()
	data, err := json.Marshal(store.Session)
	store.mu.RUnlock()
	if store.afterSnapshot != nil {
		store.afterSnapshot()
	}
	if err != nil {
		log.Error().Str("filename", store.Filename).Err(err).Msg("Unable to marshal contents of app session file")
		return fmt.Errorf("unable to marshal session: %w", err)
	}

	// Write to temp file first for atomic operation
	dir := filepath.Dir(store.Filename)
	tempFile, err := os.CreateTemp(dir, ".session-*.tmp")
	if err != nil {
		log.Error().Str("filename", store.Filename).Err(err).Msg("Unable to create temp file for session")
		return fmt.Errorf("unable to create temp file: %w", err)
	}
	tempName := tempFile.Name()

	// Clean up temp file on any error
	defer func() {
		if tempName != "" {
			os.Remove(tempName)
		}
	}()

	// Set secure permissions (owner read/write only) - SECURITY FIX
	if err := tempFile.Chmod(0600); err != nil {
		tempFile.Close()
		log.Error().Err(err).Msg("Unable to set session file permissions")
		return fmt.Errorf("unable to set file permissions: %w", err)
	}

	if _, err := tempFile.Write(data); err != nil {
		tempFile.Close()
		log.Error().Str("filename", store.Filename).Err(err).Msg("Unable to write to session file")
		return fmt.Errorf("unable to write session: %w", err)
	}

	if err := tempFile.Sync(); err != nil {
		tempFile.Close()
		log.Error().Err(err).Msg("Unable to sync session file")
		return fmt.Errorf("unable to sync session file: %w", err)
	}

	if err := tempFile.Close(); err != nil {
		log.Error().Err(err).Msg("Unable to close session file")
		return fmt.Errorf("unable to close session file: %w", err)
	}

	// Atomic rename - preferred method
	if err := os.Rename(tempName, store.Filename); err != nil {
		// Rename can fail on some Docker volume types (NFS, overlayfs)
		// Fall back to copy + remove approach
		log.Debug().Err(err).Msg("Atomic rename failed, falling back to copy")

		// Read the temp file content
		content, readErr := os.ReadFile(tempName)
		if readErr != nil {
			log.Error().Err(readErr).Msg("Unable to read temp file for fallback")
			return fmt.Errorf("unable to rename session file: %w", err)
		}

		// Write directly to target file
		if writeErr := os.WriteFile(store.Filename, content, 0600); writeErr != nil {
			log.Error().Err(writeErr).Msg("Unable to write session file (fallback)")
			return fmt.Errorf("unable to write session file: %w", writeErr)
		}

		// Temp file will be cleaned up by defer
		log.Debug().Str("filename", store.Filename).Msg("Session saved successfully (fallback method)")
		return nil
	}

	// Clear temp name so defer doesn't try to remove it
	tempName = ""

	log.Debug().Str("filename", store.Filename).Msg("Session saved successfully")
	return nil
}

// InitSessionStore - Initializes new application session store
// Returns error if session file path is invalid or cannot be loaded
func InitSessionStore(sessionFile string) (*Store, error) {
	sessionStore := NewSessionStore()

	// Load previous state of the application from session file
	if sessionFile != "" {
		absFileName, err := filepath.Abs(sessionFile)
		if err != nil {
			log.Error().Str("path", sessionFile).Err(err).Msg("Unable to retrieve absolute file path")
			return nil, fmt.Errorf("invalid session file path: %w", err)
		}

		sessionStore.Filename = absFileName
		if err := sessionStore.Load(); err != nil {
			// Log but don't fail - we can continue with empty session
			log.Warn().Err(err).Msg("Failed to load session, starting fresh")
		}
	}

	return sessionStore, nil
}
