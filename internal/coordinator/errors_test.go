package coordinator

import (
	"errors"
	"fmt"
	"testing"

	"github.com/munhq/gpuscale/pkg/provider"
)

func TestClassifyError_Nil(t *testing.T) {
	if got := ClassifyError(nil); got != ErrorNone {
		t.Errorf("nil error: want ErrorNone, got %v", got)
	}
}

func TestClassifyError_OfferExpiredSentinel(t *testing.T) {
	if got := ClassifyError(provider.ErrOfferExpired); got != ErrorExpired {
		t.Errorf("ErrOfferExpired: want ErrorExpired, got %v", got)
	}
}

func TestClassifyError_InstanceNotFoundSentinel(t *testing.T) {
	if got := ClassifyError(provider.ErrInstanceNotFound); got != ErrorExpired {
		t.Errorf("ErrInstanceNotFound: want ErrorExpired, got %v", got)
	}
}

func TestClassifyError_WrappedSentinel(t *testing.T) {
	wrapped := fmt.Errorf("provision failed: %w", provider.ErrOfferExpired)
	if got := ClassifyError(wrapped); got != ErrorExpired {
		t.Errorf("wrapped ErrOfferExpired: want ErrorExpired, got %v", got)
	}
}

func TestClassifyError_RateLimited(t *testing.T) {
	cases := []string{
		"http 429 too many requests",
		"request failed: 429",
		"Too Many Requests from API",
	}
	for _, msg := range cases {
		got := ClassifyError(errors.New(msg))
		if got != ErrorRateLimited {
			t.Errorf("rate limit msg %q: want ErrorRateLimited, got %v", msg, got)
		}
	}
}

func TestClassifyError_Conflict(t *testing.T) {
	cases := []string{
		"no_such_ask: offer gone",
		"gpu already rented",
		"gpu conflict detected",
	}
	for _, msg := range cases {
		got := ClassifyError(errors.New(msg))
		if got != ErrorConflict {
			t.Errorf("conflict msg %q: want ErrorConflict, got %v", msg, got)
		}
	}
}

func TestClassifyError_InvalidRequest(t *testing.T) {
	cases := []string{
		"invalid_request: image not supported",
		"operating system is not valid for this instance",
	}
	for _, msg := range cases {
		got := ClassifyError(errors.New(msg))
		if got != ErrorExpired {
			t.Errorf("invalid_request msg %q: want ErrorExpired (blacklist), got %v", msg, got)
		}
	}
}

func TestClassifyError_Permanent(t *testing.T) {
	cases := []string{
		"401 unauthorized",
		"403 forbidden",
		"insufficient_credit balance",
		"account lacks credit",
		"billing issue detected",
		"payment required",
	}
	for _, msg := range cases {
		got := ClassifyError(errors.New(msg))
		if got != ErrorPermanent {
			t.Errorf("permanent msg %q: want ErrorPermanent, got %v", msg, got)
		}
	}
}

func TestClassifyError_DefaultTransient(t *testing.T) {
	cases := []string{
		"connection reset by peer",
		"context deadline exceeded",
		"i/o timeout",
		"unexpected EOF",
	}
	for _, msg := range cases {
		got := ClassifyError(errors.New(msg))
		if got != ErrorTransient {
			t.Errorf("transient msg %q: want ErrorTransient, got %v", msg, got)
		}
	}
}
