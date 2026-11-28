package hub

import "testing"

func TestTokenManagerReconcileSyncsToDurableToken(t *testing.T) {
	m := NewTokenManager()
	m.SetLookup(func(sessionID string) (string, bool) {
		if sessionID == "s1" {
			return "durable-token", true
		}
		return "", false
	})
	issued := m.Issue("s1")
	if issued == "durable-token" {
		t.Fatal("test setup: issued token equals the durable token")
	}
	// Before Reconcile only the issued (in-memory) token verifies.
	if !m.Verify("s1", issued) {
		t.Fatal("issued token does not verify before Reconcile")
	}
	if m.Verify("s1", "durable-token") {
		t.Fatal("durable token verifies before Reconcile, want only the issued token valid")
	}

	if !m.Reconcile("s1") {
		t.Fatal("Reconcile returned false, want true (durable token present)")
	}
	if !m.Verify("s1", "durable-token") {
		t.Fatal("durable token does not verify after Reconcile")
	}
	if m.Verify("s1", issued) {
		t.Fatal("stale issued token still verifies after Reconcile")
	}

	if m.Reconcile("unknown") {
		t.Fatal("Reconcile for a session without a durable token returned true")
	}

	noLookup := NewTokenManager()
	noLookup.Issue("s1")
	if noLookup.Reconcile("s1") {
		t.Fatal("Reconcile without a durable lookup returned true")
	}
}
