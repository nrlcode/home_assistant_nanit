package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/indiefan/home_assistant_nanit/pkg/baby"
	"github.com/indiefan/home_assistant_nanit/pkg/message"
	"github.com/indiefan/home_assistant_nanit/pkg/session"
	"github.com/indiefan/home_assistant_nanit/pkg/utils"
	"github.com/rs/zerolog/log"
)

const (
	// MaxResponseBodySize limits response body to 10MB to prevent DoS
	MaxResponseBodySize = 10 * 1024 * 1024
)

var myClient = &http.Client{Timeout: 30 * time.Second}

var ErrExpiredRefreshToken = errors.New("refresh token has expired, relogin required")
var ErrTransientFailure = errors.New("transient HTTP failure")
var ErrAuthorizationFailed = errors.New("authorization failed")
var ErrUnexpectedStatusCode = errors.New("unexpected status code")

// ------------------------------------------

type authResponsePayload struct {
	AccessToken  string `json:"access_token,omitempty"`
	RefreshToken string `json:"refresh_token,omitempty"`
}

type babiesResponsePayload struct {
	Babies []baby.Baby `json:"babies"`
}

type messagesResponsePayload struct {
	Messages []message.Message `json:"messages"`
}

// ------------------------------------------

// NanitClient - client context
type NanitClient struct {
	authMu       sync.Mutex // Protects authorization flow
	Email        string
	Password     string
	RefreshToken string
	SessionStore *session.Store
}

// MaybeAuthorize - Performs authorization if we don't have token or we assume it is expired
func (c *NanitClient) MaybeAuthorize(force bool) error {
	return c.MaybeAuthorizeCtx(context.Background(), force)
}

// lockAuthMu acquires the authorization serialization lock, aborting
// promptly when ctx is cancelled while another owner holds it.
func (c *NanitClient) lockAuthMu(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	for {
		if c.authMu.TryLock() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// MaybeAuthorizeCtx - context-aware authorization for polling cancellation.
func (c *NanitClient) MaybeAuthorizeCtx(ctx context.Context, force bool) error {
	if err := c.lockAuthMu(ctx); err != nil {
		return err
	}
	defer c.authMu.Unlock()

	if force || c.SessionStore.GetAuthToken() == "" || time.Since(c.SessionStore.GetAuthTime()) > AuthTokenTimelife {
		return c.authorizeInternalCtx(ctx)
	}
	return nil
}

// Authorize - performs authorization attempt (thread-safe)
func (c *NanitClient) Authorize() error {
	return c.AuthorizeCtx(context.Background())
}

// AuthorizeCtx - context-aware authorization attempt (thread-safe).
func (c *NanitClient) AuthorizeCtx(ctx context.Context) error {
	if err := c.lockAuthMu(ctx); err != nil {
		return err
	}
	defer c.authMu.Unlock()
	return c.authorizeInternalCtx(ctx)
}

// authorizeInternal - performs authorization (must be called with authMu held)
func (c *NanitClient) authorizeInternal() error {
	return c.authorizeInternalCtx(context.Background())
}

// authorizeInternalCtx - performs authorization (must be called with authMu held)
func (c *NanitClient) authorizeInternalCtx(ctx context.Context) error {
	c.SessionStore.EnsureRefreshToken(c.RefreshToken)

	if c.SessionStore.GetRefreshToken() != "" {
		err := c.renewSessionInternalCtx(ctx)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !errors.Is(err, ErrExpiredRefreshToken) {
			log.Error().Err(err).Msg("Error occurred while trying to refresh the session")
			return fmt.Errorf("session refresh failed: %w", err)
		}
		// Refresh token expired, fall through to login
	}

	return c.loginInternalCtx(ctx)
}

// RenewSession renews an existing session using a valid refresh token (thread-safe)
func (c *NanitClient) RenewSession() error {
	return c.RenewSessionCtx(context.Background())
}

// RenewSessionCtx renews an existing session (thread-safe, context-aware).
func (c *NanitClient) RenewSessionCtx(ctx context.Context) error {
	if err := c.lockAuthMu(ctx); err != nil {
		return err
	}
	defer c.authMu.Unlock()
	return c.renewSessionInternalCtx(ctx)
}

// renewSessionInternal renews session (must be called with authMu held)
func (c *NanitClient) renewSessionInternal() error {
	return c.renewSessionInternalCtx(context.Background())
}

// renewSessionInternalCtx renews session (must be called with authMu held)
func (c *NanitClient) renewSessionInternalCtx(ctx context.Context) error {
	requestBody, err := json.Marshal(map[string]string{
		"refresh_token": c.SessionStore.GetRefreshToken(),
	})
	if err != nil {
		return fmt.Errorf("unable to marshal auth body: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", "https://api.nanit.com/tokens/refresh", bytes.NewBuffer(requestBody))
	if err != nil {
		return fmt.Errorf("unable to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	r, err := myClient.Do(req)
	if err != nil {
		return fmt.Errorf("unable to renew session: %w", err)
	}
	defer r.Body.Close()

	if r.StatusCode == 404 {
		log.Warn().Msg("Server responded with code 404 - refresh token has expired")
		return ErrExpiredRefreshToken
	} else if r.StatusCode < 200 || r.StatusCode > 299 {
		return fmt.Errorf("%w: %d", ErrUnexpectedStatusCode, r.StatusCode)
	}

	// Limit response body size
	limitedReader := io.LimitReader(r.Body, MaxResponseBodySize)
	authResponse := new(authResponsePayload)
	if err := json.NewDecoder(limitedReader).Decode(authResponse); err != nil {
		return fmt.Errorf("unable to decode response: %w", err)
	}

	log.Info().Str("token", utils.AnonymizeToken(authResponse.AccessToken, 4)).Msg("Authorized")
	c.SessionStore.UpdateAuth(authResponse.AccessToken, authResponse.RefreshToken)

	if err := c.SessionStore.Save(); err != nil {
		log.Warn().Err(err).Msg("Failed to save session after renewal")
	}

	return nil
}

// Login performs login with email/password (thread-safe)
func (c *NanitClient) Login() error {
	return c.LoginCtx(context.Background())
}

// LoginCtx performs login with email/password (thread-safe, context-aware).
func (c *NanitClient) LoginCtx(ctx context.Context) error {
	if err := c.lockAuthMu(ctx); err != nil {
		return err
	}
	defer c.authMu.Unlock()
	return c.loginInternalCtx(ctx)
}

// loginInternal performs login (must be called with authMu held)
func (c *NanitClient) loginInternal() error {
	return c.loginInternalCtx(context.Background())
}

// loginInternalCtx performs login (must be called with authMu held)
func (c *NanitClient) loginInternalCtx(ctx context.Context) error {
	// SECURITY: Don't log email address
	log.Info().Msg("Authorizing using user credentials")

	requestBody, err := json.Marshal(map[string]string{
		"email":    c.Email,
		"password": c.Password,
	})
	if err != nil {
		return fmt.Errorf("unable to marshal auth body: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", "https://api.nanit.com/login", bytes.NewBuffer(requestBody))
	if err != nil {
		return fmt.Errorf("unable to create request: %w", err)
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("nanit-api-version", "1")

	r, err := myClient.Do(req)
	if err != nil {
		return fmt.Errorf("unable to fetch auth token: %w", err)
	}
	defer r.Body.Close()

	if r.StatusCode == 401 {
		return fmt.Errorf("%w: invalid credentials or 2FA required", ErrAuthorizationFailed)
	} else if r.StatusCode != 201 {
		return fmt.Errorf("%w: %d", ErrUnexpectedStatusCode, r.StatusCode)
	}

	limitedReader := io.LimitReader(r.Body, MaxResponseBodySize)
	authResponse := new(authResponsePayload)
	if err := json.NewDecoder(limitedReader).Decode(authResponse); err != nil {
		return fmt.Errorf("unable to decode response: %w", err)
	}

	log.Info().Str("token", utils.AnonymizeToken(authResponse.AccessToken, 4)).Msg("Authorized")
	c.SessionStore.UpdateAuth(authResponse.AccessToken, authResponse.RefreshToken)

	if err := c.SessionStore.Save(); err != nil {
		log.Warn().Err(err).Msg("Failed to save session after login")
	}

	return nil
}

// FetchAuthorized - makes authorized http request
func (c *NanitClient) FetchAuthorized(req *http.Request, data interface{}) error {
	return c.FetchAuthorizedCtx(req.Context(), req, data)
}

// FetchAuthorizedCtx - context-aware authorized request; polling cancellation
// aborts the in-flight GET and the 401/absent-token refresh/login retry.
func (c *NanitClient) FetchAuthorizedCtx(ctx context.Context, req *http.Request, data interface{}) error {
	// Clone request body for potential retry (body is consumed on first attempt)
	var bodyBytes []byte
	if req.Body != nil {
		var err error
		bodyBytes, err = io.ReadAll(req.Body)
		req.Body.Close()
		if err != nil {
			return fmt.Errorf("unable to read request body: %w", err)
		}
	}

	for i := 0; i < 2; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Restore body for each attempt
		if bodyBytes != nil {
			req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
		}

		token := c.SessionStore.GetAuthToken()
		if token != "" {
			req.Header.Set("Authorization", token)

			res, err := myClient.Do(req.WithContext(ctx))
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return fmt.Errorf("HTTP request failed: %w", err)
			}

			if res.StatusCode != 401 {
				if res.StatusCode != 200 {
					res.Body.Close()
					return fmt.Errorf("%w: %d", ErrUnexpectedStatusCode, res.StatusCode)
				}

				limitedReader := io.LimitReader(res.Body, MaxResponseBodySize)
				decodeErr := json.NewDecoder(limitedReader).Decode(data)
				res.Body.Close()
				if decodeErr != nil {
					return fmt.Errorf("unable to decode response: %w", decodeErr)
				}

				return nil
			}

			res.Body.Close()
			log.Info().Msg("Token might be expired. Will try to re-authenticate.")
		}

		if err := c.AuthorizeCtx(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	return fmt.Errorf("%w: failed after 2 attempts", ErrAuthorizationFailed)
}

// TryFetchAuthorized - makes authorized http request, returns error on transient failures
func (c *NanitClient) TryFetchAuthorized(req *http.Request, data interface{}) error {
	return c.TryFetchAuthorizedCtx(req.Context(), req, data)
}

// TryFetchAuthorizedCtx - context-aware authorized request for polling.
func (c *NanitClient) TryFetchAuthorizedCtx(ctx context.Context, req *http.Request, data interface{}) error {
	// Clone request body for potential retry (body is consumed on first attempt)
	var bodyBytes []byte
	if req.Body != nil {
		var err error
		bodyBytes, err = io.ReadAll(req.Body)
		req.Body.Close()
		if err != nil {
			return fmt.Errorf("%w: unable to read request body: %v", ErrTransientFailure, err)
		}
	}

	for i := 0; i < 2; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Restore body for each attempt
		if bodyBytes != nil {
			req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
		}

		token := c.SessionStore.GetAuthToken()
		if token != "" {
			req.Header.Set("Authorization", token)

			res, err := myClient.Do(req.WithContext(ctx))
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				log.Warn().Err(err).Msg("HTTP request failed (transient)")
				return fmt.Errorf("%w: %v", ErrTransientFailure, err)
			}

			if res.StatusCode != 401 {
				if res.StatusCode != 200 {
					res.Body.Close()
					log.Warn().Int("code", res.StatusCode).Msg("Server responded with unexpected status code (transient)")
					return fmt.Errorf("%w: status code %d", ErrTransientFailure, res.StatusCode)
				}

				limitedReader := io.LimitReader(res.Body, MaxResponseBodySize)
				decodeErr := json.NewDecoder(limitedReader).Decode(data)
				res.Body.Close()
				if decodeErr != nil {
					log.Warn().Err(decodeErr).Msg("Unable to decode response (transient)")
					return fmt.Errorf("%w: %v", ErrTransientFailure, decodeErr)
				}

				return nil
			}

			res.Body.Close()
			log.Info().Msg("Token might be expired. Will try to re-authenticate.")
		}

		if err := c.AuthorizeCtx(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	return fmt.Errorf("%w: failed authorization after 2 attempts", ErrTransientFailure)
}

// FetchBabies - fetches baby list
func (c *NanitClient) FetchBabies() ([]baby.Baby, error) {
	log.Info().Msg("Fetching babies list")
	req, err := http.NewRequest("GET", "https://api.nanit.com/babies", nil)
	if err != nil {
		return nil, fmt.Errorf("unable to create request: %w", err)
	}

	data := new(babiesResponsePayload)
	if err := c.FetchAuthorized(req, data); err != nil {
		return nil, err
	}

	c.SessionStore.UpdateBabies(data.Babies)
	if err := c.SessionStore.Save(); err != nil {
		log.Warn().Err(err).Msg("Failed to save session after fetching babies")
	}
	return data.Babies, nil
}

// FetchMessages - fetches message list
func (c *NanitClient) FetchMessages(babyUID string, limit int) ([]message.Message, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("https://api.nanit.com/babies/%s/messages?limit=%d", babyUID, limit), nil)
	if err != nil {
		return nil, fmt.Errorf("unable to create request: %w", err)
	}

	data := new(messagesResponsePayload)
	if err := c.FetchAuthorized(req, data); err != nil {
		return nil, err
	}

	return data.Messages, nil
}

// TryFetchMessages - fetches message list, returns error on transient failures
func (c *NanitClient) TryFetchMessages(babyUID string, limit int) ([]message.Message, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("https://api.nanit.com/babies/%s/messages?limit=%d", babyUID, limit), nil)
	if err != nil {
		return nil, fmt.Errorf("unable to create request: %w", err)
	}

	data := new(messagesResponsePayload)
	if err := c.TryFetchAuthorized(req, data); err != nil {
		return nil, err
	}

	return data.Messages, nil
}

// TryFetchMessagesCtx - context-aware fetch; request is bound to ctx so
// cancellation aborts the in-flight HTTP call.
func (c *NanitClient) TryFetchMessagesCtx(ctx context.Context, babyUID string, limit int) ([]message.Message, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("https://api.nanit.com/babies/%s/messages?limit=%d", babyUID, limit), nil)
	if err != nil {
		return nil, fmt.Errorf("unable to create request: %w", err)
	}

	data := new(messagesResponsePayload)
	if err := c.TryFetchAuthorizedCtx(ctx, req, data); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	return data.Messages, nil
}

// FetchNewMessagesCtx - bounded recent window with error/context propagation.
func (c *NanitClient) FetchNewMessagesCtx(ctx context.Context, babyUID string, defaultMessageTimeout time.Duration) ([]message.Message, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	fetchedMessages, err := c.TryFetchMessagesCtx(ctx, babyUID, 10)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if len(fetchedMessages) == 0 {
		log.Debug().Msg("No messages fetched")
		return []message.Message{}, nil
	}

	// Log all message types for debugging (helps identify unknown event types)
	for _, msg := range fetchedMessages {
		log.Debug().
			Str("baby_uid", babyUID).
			Int("id", msg.Id).
			Str("type", msg.Type).
			Time("time", msg.Time.Time()).
			Msg("Fetched message from API")
	}

	sort.Slice(fetchedMessages, func(i, j int) bool {
		return fetchedMessages[i].Time.Time().After(fetchedMessages[j].Time.Time())
	})

	// Bounded recent window only; no shared persisted watermark is read or
	// advanced here so concurrent per-baby pollers cannot interfere.
	cutoff := time.Now().UTC().Add(-defaultMessageTimeout)

	filteredMessages := message.FilterMessages(fetchedMessages, func(msg message.Message) bool {
		return msg.Time.Time().After(cutoff)
	})

	log.Debug().Msgf("Found %d new messages", len(filteredMessages))

	return filteredMessages, nil
}

// EnsureBabies - fetches baby list if not fetched already
func (c *NanitClient) EnsureBabies() ([]baby.Baby, error) {
	if babies := c.SessionStore.GetBabies(); len(babies) == 0 {
		return c.FetchBabies()
	}
	return c.SessionStore.GetBabies(), nil
}

// FetchNewMessages - fetches the bounded recent message window, ignoring old
// messages. It never reads or advances the persisted LastSeenMessageTime
// cursor (left readable for compatibility); per-baby dedup at the polling
// owner filters repeats. Returns empty list on transient errors.
func (c *NanitClient) FetchNewMessages(babyUID string, defaultMessageTimeout time.Duration) []message.Message {
	fetchedMessages, err := c.TryFetchMessages(babyUID, 10)
	newMessages := make([]message.Message, 0)

	if err != nil {
		log.Warn().Err(err).Str("baby_uid", babyUID).Msg("Failed to fetch messages, will retry")
		return newMessages
	}

	if len(fetchedMessages) == 0 {
		log.Debug().Msg("No messages fetched")
		return newMessages
	}

	// Log all message types for debugging (helps identify unknown event types)
	for _, msg := range fetchedMessages {
		log.Debug().
			Str("baby_uid", babyUID).
			Int("id", msg.Id).
			Str("type", msg.Type).
			Time("time", msg.Time.Time()).
			Msg("Fetched message from API")
	}

	sort.Slice(fetchedMessages, func(i, j int) bool {
		return fetchedMessages[i].Time.Time().After(fetchedMessages[j].Time.Time())
	})

	// Bounded recent window only; no shared persisted watermark is read or
	// advanced here so concurrent per-baby pollers cannot interfere.
	cutoff := time.Now().UTC().Add(-defaultMessageTimeout)

	filteredMessages := message.FilterMessages(fetchedMessages, func(msg message.Message) bool {
		return msg.Time.Time().After(cutoff)
	})

	log.Debug().Msgf("Found %d new messages", len(filteredMessages))

	return filteredMessages
}
