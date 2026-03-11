package coordinator

import (
	"testing"
	"time"

	"github.com/munhq/gpuscale/pkg/provider"
)

func TestOfferBlacklist_AddAndFilter(t *testing.T) {
	bl := NewOfferBlacklist(10 * time.Minute)

	offers := []provider.Offer{
		{ProviderName: "vastai", OfferID: "offer-1"},
		{ProviderName: "vastai", OfferID: "offer-2"},
		{ProviderName: "vastai", OfferID: "offer-3"},
	}

	bl.Add("vastai", "offer-1")

	filtered := bl.Filter(offers)
	if len(filtered) != 2 {
		t.Errorf("after blacklisting offer-1: want 2 offers, got %d", len(filtered))
	}
	for _, o := range filtered {
		if o.OfferID == "offer-1" {
			t.Error("blacklisted offer-1 should not appear in filtered results")
		}
	}
}

func TestOfferBlacklist_SizeCountsActiveOnly(t *testing.T) {
	bl := NewOfferBlacklist(10 * time.Minute)
	bl.Add("p", "a")
	bl.Add("p", "b")
	bl.Add("p", "c")

	if s := bl.Size(); s != 3 {
		t.Errorf("Size after 3 adds: want 3, got %d", s)
	}
}

func TestOfferBlacklist_TTLExpiry(t *testing.T) {
	bl := NewOfferBlacklist(1 * time.Millisecond)
	bl.Add("vastai", "offer-expired")

	time.Sleep(5 * time.Millisecond)

	offers := []provider.Offer{
		{ProviderName: "vastai", OfferID: "offer-expired"},
	}
	filtered := bl.Filter(offers)
	if len(filtered) != 1 {
		t.Errorf("expired entry should pass through filter: want 1, got %d", len(filtered))
	}

	if s := bl.Size(); s != 0 {
		t.Errorf("expired entries should not count toward size: want 0, got %d", s)
	}
}

func TestOfferBlacklist_AddWithTTL(t *testing.T) {
	bl := NewOfferBlacklist(10 * time.Minute)
	bl.AddWithTTL("vastai", "short-lived", 1*time.Millisecond)
	bl.Add("vastai", "long-lived")

	time.Sleep(5 * time.Millisecond)

	offers := []provider.Offer{
		{ProviderName: "vastai", OfferID: "short-lived"},
		{ProviderName: "vastai", OfferID: "long-lived"},
	}
	filtered := bl.Filter(offers)
	if len(filtered) != 1 || filtered[0].OfferID != "short-lived" {
		t.Errorf("short-lived should be expired, long-lived should be blocked: got %v", filtered)
	}
}

func TestOfferBlacklist_DifferentProviders(t *testing.T) {
	bl := NewOfferBlacklist(10 * time.Minute)
	bl.Add("vastai", "offer-1")

	offers := []provider.Offer{
		{ProviderName: "vastai", OfferID: "offer-1"},
		{ProviderName: "verda", OfferID: "offer-1"}, // same offer ID, different provider
	}
	filtered := bl.Filter(offers)
	if len(filtered) != 1 || filtered[0].ProviderName != "verda" {
		t.Errorf("blacklist is provider-scoped: verda offer-1 should pass, got %v", filtered)
	}
}

func TestOfferBlacklist_EmptyInput(t *testing.T) {
	bl := NewOfferBlacklist(10 * time.Minute)
	bl.Add("vastai", "x")

	filtered := bl.Filter(nil)
	if len(filtered) != 0 {
		t.Errorf("nil input: want 0, got %d", len(filtered))
	}
}

func TestOfferBlacklist_FilterWithNoBlacklisted(t *testing.T) {
	bl := NewOfferBlacklist(10 * time.Minute)

	offers := []provider.Offer{
		{ProviderName: "vastai", OfferID: "a"},
		{ProviderName: "vastai", OfferID: "b"},
	}
	filtered := bl.Filter(offers)
	if len(filtered) != 2 {
		t.Errorf("no blacklisted entries: want all 2, got %d", len(filtered))
	}
}
