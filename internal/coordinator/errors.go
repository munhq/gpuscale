package coordinator

import (
	"errors"
	"strings"

	"github.com/munhq/gpuscale/pkg/provider"
)

// ErrorCategory classifies provider errors for retry/blacklist decisions.
type ErrorCategory int

const (
	ErrorNone        ErrorCategory = iota
	ErrorExpired                   // offer no longer available → blacklist
	ErrorConflict                  // offer rented by someone else → blacklist
	ErrorRateLimited               // 429 Too Many Requests → back off, invalidate cache
	ErrorTransient                 // network, timeout, 5xx → try next offer
	ErrorPermanent                 // invalid request, auth failure → stop retrying
)

// ClassifyError maps a provider error to a category for retry/blacklist behavior.
// Uses sentinel errors from pkg/provider and string pattern matching for HTTP
// status codes embedded in error messages.
func ClassifyError(err error) ErrorCategory {
	if err == nil {
		return ErrorNone
	}
	if errors.Is(err, provider.ErrOfferExpired) {
		return ErrorExpired
	}
	if errors.Is(err, provider.ErrInstanceNotFound) {
		return ErrorExpired
	}

	msg := strings.ToLower(err.Error())

	if strings.Contains(msg, "429") || strings.Contains(msg, "too many requests") {
		return ErrorRateLimited
	}
	if strings.Contains(msg, "no_such_ask") || strings.Contains(msg, "already rented") ||
		strings.Contains(msg, "gpu conflict") {
		return ErrorConflict
	}
	if strings.Contains(msg, "401") || strings.Contains(msg, "403") ||
		strings.Contains(msg, "unauthorized") || strings.Contains(msg, "forbidden") {
		return ErrorPermanent
	}

	// Default to transient — safe fallback that triggers retry.
	return ErrorTransient
}
