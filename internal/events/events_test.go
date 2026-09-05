package events

import "testing"

func TestNormalize_FillsDefaults(t *testing.T) {
	got, err := Normalize(Event{UserID: "u1", Type: TypeClick})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.EventID == "" {
		t.Error("expected event_id to be generated")
	}
	if got.Timestamp.IsZero() {
		t.Error("expected timestamp to be generated")
	}
}

func TestNormalize_Rejections(t *testing.T) {
	cases := []struct {
		name string
		in   Event
	}{
		{"missing user id", Event{Type: TypeClick}},
		{"missing type", Event{UserID: "u1"}},
		{"invalid type", Event{UserID: "u1", Type: "bogus"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := Normalize(c.in); err == nil {
				t.Error("expected an error, got nil")
			}
		})
	}
}

func TestNormalize_PreservesSuppliedIDAndTime(t *testing.T) {
	e := Event{EventID: "fixed-id", UserID: "u1", Type: TypePurchase}
	got, err := Normalize(e)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.EventID != "fixed-id" {
		t.Errorf("expected event id to be preserved, got %q", got.EventID)
	}
}
