// Package events defines the behavior event schema accepted by the
// ingestion API and consumed by the profile builder, plus the validation
// rules applied at the edge (ingestion) so bad data never reaches Kafka.
package events

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Type enumerates the event categories the pipeline understands. Any
// other value is rejected by Validate -- the strategy engine and
// profile builder both switch on these constants.
type Type string

const (
	TypePageView  Type = "page_view"
	TypeClick     Type = "click"
	TypePurchase  Type = "purchase"
	TypeLifecycle Type = "lifecycle" // e.g. signup, subscription_cancelled, reactivated
)

func (t Type) valid() bool {
	switch t {
	case TypePageView, TypeClick, TypePurchase, TypeLifecycle:
		return true
	default:
		return false
	}
}

// Event is the wire format for both the ingestion API request body and
// the Kafka message value. Properties is intentionally a free-form bag
// so new event shapes don't require a schema migration.
type Event struct {
	EventID    string         `json:"event_id"`
	UserID     string         `json:"user_id"`
	Type       Type           `json:"type"`
	Timestamp  time.Time      `json:"timestamp"`
	Properties map[string]any `json:"properties,omitempty"`
}

var (
	ErrMissingUserID = errors.New("user_id is required")
	ErrMissingType   = errors.New("type is required")
	ErrInvalidType   = errors.New("type must be one of page_view, click, purchase, lifecycle")
)

// Normalize fills in server-assigned fields (event id, timestamp) when
// the caller omitted them, and validates everything else. Callers should
// use the returned event, not the original, since defaults are applied
// on a copy.
func Normalize(e Event) (Event, error) {
	if e.UserID == "" {
		return Event{}, ErrMissingUserID
	}
	if e.Type == "" {
		return Event{}, ErrMissingType
	}
	if !e.Type.valid() {
		return Event{}, fmt.Errorf("%w: got %q", ErrInvalidType, e.Type)
	}
	if e.EventID == "" {
		e.EventID = uuid.NewString()
	}
	if e.Timestamp.IsZero() {
		e.Timestamp = time.Now().UTC()
	} else {
		e.Timestamp = e.Timestamp.UTC()
	}
	return e, nil
}
