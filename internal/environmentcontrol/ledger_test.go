package environmentcontrol

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/environment"
	"github.com/tmc/skygo/internal/store"
)

func testLedger() ledger {
	at := time.Date(2026, 10, 24, 12, 0, 0, 5, time.UTC)
	return ledger{
		Nonce:  strings.Repeat("a", 32),
		Native: []byte("uid"),
		Idle:   at,
		Sessions: []session{
			{ID: strings.Repeat("1", 32), Kind: execSession, Owner: strings.Repeat("b", 32), Started: at, Unknown: true},
			{ID: strings.Repeat("2", 32), Kind: forwardSession, Owner: strings.Repeat("b", 32), Started: at},
		},
	}
}

func TestLedgerRoundTrip(t *testing.T) {
	data, err := encodeLedger(testLedger())
	if err != nil {
		t.Fatal(err)
	}
	l, err := decodeLedger(data)
	if err != nil {
		t.Fatal(err)
	}
	again, err := encodeLedger(l)
	if err != nil || !bytes.Equal(again, data) {
		t.Fatalf("re-encoded %s, %v", again, err)
	}
	// The smallest ledger is accepted.
	if _, err := encodeLedger(ledger{Nonce: strings.Repeat("c", 32), Idle: time.Unix(1, 0).UTC()}); err != nil {
		t.Fatal(err)
	}
}

func TestLedgerRefusals(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*ledger)
	}{
		{"nonce", func(l *ledger) { l.Nonce = "A" + l.Nonce[1:] }},
		{"native", func(l *ledger) { l.Native = make([]byte, MaxNativeBytes+1) }},
		{"zero idle", func(l *ledger) { l.Idle = time.Time{} }},
		{"local idle", func(l *ledger) { l.Idle = l.Idle.In(time.FixedZone("x", 3600)) }},
		{"session id", func(l *ledger) { l.Sessions[0].ID = "1" }},
		{"owner", func(l *ledger) { l.Sessions[0].Owner = "" }},
		{"kind", func(l *ledger) { l.Sessions[0].Kind = "shell" }},
		{"unknown forward", func(l *ledger) { l.Sessions[1].Unknown = true }},
		{"zero start", func(l *ledger) { l.Sessions[1].Started = time.Time{} }},
		{"duplicate", func(l *ledger) { l.Sessions[1].ID = l.Sessions[0].ID }},
		{"too many", func(l *ledger) {
			for i := len(l.Sessions); i <= MaxSessions; i++ {
				s := l.Sessions[1]
				s.ID = strings.Repeat(string("3456789abcdef"[i]), 32)
				l.Sessions = append(l.Sessions, s)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			l := testLedger()
			test.change(&l)
			if _, err := encodeLedger(l); err == nil {
				t.Fatal("encodeLedger accepted")
			}
		})
	}
}

func TestLedgerDecodeRefusesOtherBytes(t *testing.T) {
	data, err := encodeLedger(testLedger())
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		data []byte
	}{
		{"empty", nil},
		{"trailing", append(bytes.Clone(data), []byte("{}")...)},
		{"space", append([]byte(" "), data...)},
		{"unknown field", bytes.Replace(data, []byte(`{"nonce"`), []byte(`{"extra":1,"nonce"`), 1)},
		{"later unknown field", bytes.Replace(data, []byte(`"sessions":[`), []byte(`"sessions":[],"x":[`), 1)},
		{"explicit false", bytes.Replace(data, []byte(`"idle"`), []byte(`"expired":false,"idle"`), 1)},
		{"uppercase hex", bytes.Replace(data, []byte(strings.Repeat("a", 32)), []byte(strings.Repeat("A", 32)), 1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := decodeLedger(test.data); err == nil {
				t.Fatalf("decodeLedger accepted %s", test.data)
			}
		})
	}
	// The largest ledger, with the longest times, fits its bound, which
	// fits a checkpoint.
	late := time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)
	l := ledger{Nonce: strings.Repeat("f", 32), Native: make([]byte, MaxNativeBytes), Idle: late, Expired: true}
	for i := range MaxSessions {
		l.Sessions = append(l.Sessions, session{ID: strings.Repeat(string("0123456789abcdef"[i]), 32), Kind: execSession, Owner: strings.Repeat("f", 32), Started: late, Unknown: true})
	}
	data, err = encodeLedger(l)
	if err != nil || len(data) > maxLedgerBytes || maxLedgerBytes > environment.MaxCheckpointBytes {
		t.Fatalf("maximal ledger %d bytes, bound %d: %v", len(data), maxLedgerBytes, err)
	}
}

func TestDecodeRefusesOtherEnvelopes(t *testing.T) {
	data, err := encodeLedger(testLedger())
	if err != nil {
		t.Fatal(err)
	}
	c := &Controller{kind: "fake-pod"}
	for _, test := range []struct {
		name string
		cp   environment.Checkpoint
		ok   bool
	}{
		{"current", environment.Checkpoint{Kind: "fake-pod", Version: checkpointVersion, Data: data}, true},
		{"unknown version", environment.Checkpoint{Kind: "fake-pod", Version: checkpointVersion + 1, Data: data}, false},
		{"other kind", environment.Checkpoint{Kind: "ec2-vm", Version: checkpointVersion, Data: data}, false},
		{"runtime bytes", environment.Checkpoint{Kind: "fake-pod", Version: checkpointVersion, Data: []byte("uid")}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := c.decode(store.Environment{Checkpoint: test.cp})
			if (err == nil) != test.ok {
				t.Fatalf("decode: %v", err)
			}
		})
	}
}
