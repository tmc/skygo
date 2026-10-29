//go:build unix

package gce

import "testing"

// MODEL corruption, distinct from the genuine predecessor-writer corpus:
// encoding/json recognizes Op, so an aliased container must not hide an
// intent field that a version 1 writer could never have emitted.
func TestLegacyAliasedOperationIntentRefused(t *testing.T) {
	f := newFake(t)
	m, dir := newTestMachine(t, f)
	requireReached(t, m)
	if err := m.SetGoal(Stopped); err != nil {
		t.Fatal(err)
	}
	if _, err := m.prepare(kindStop); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	tamper(t, dir, func(v map[string]any) {
		v["version"] = float64(1)
		delete(v, "intent")
		delete(v, "completed_intent")
		for _, op := range v["history"].([]any) {
			delete(op.(map[string]any), "intent")
		}
		op := v["op"].(map[string]any)
		delete(op, "intent")
		op["Intent"] = float64(0)
		v["Op"] = op
		delete(v, "op")
	})
	checkRefusedBeforeHTTP(t, f, dir)
}
