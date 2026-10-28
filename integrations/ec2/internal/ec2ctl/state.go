package ec2ctl

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/tmc/skygo/integrations/ec2/internal/ec2api"
	"github.com/tmc/skygo/integrations/ec2/internal/ec2vm"
)

// The format fence of the state file. A file of another format or version
// is refused, never migrated in place.
const (
	stateFormat  = "skygo-ec2-vm-state"
	stateVersion = 1
)

var errUnsupportedState = errors.New("unsupported state format")

// A state is the durable content of a state directory.
type state struct {
	profile ec2api.Profile
	restart bool // the record wants Stopped as the first half of a restart
	rec     ec2vm.Record
}

type wireState struct {
	Format  string      `json:"format"`
	Version int         `json:"version"`
	Profile wireProfile `json:"profile"`
	Restart bool        `json:"restart_pending"`
	Record  wireRecord  `json:"record"`
}

// wireRecord is the encoding of every field of an ec2vm.Record.
type wireRecord struct {
	Owner           string         `json:"owner"`
	Environment     string         `json:"environment"`
	Generation      uint64         `json:"generation"`
	Lease           string         `json:"lease"`
	ProfileHash     string         `json:"profile_hash"`
	Request         []byte         `json:"request"`
	Token           string         `json:"token"`
	Desired         ec2vm.Desired  `json:"desired"`
	Phase           ec2vm.Phase    `json:"phase"`
	Hold            ec2vm.Hold     `json:"hold"`
	InstanceID      string         `json:"instance_id"`
	Reported        []string       `json:"reported"`
	CreateSent      bool           `json:"create_sent"`
	CreateSentAt    time.Time      `json:"create_sent_at"`
	CreditsVerified bool           `json:"credits_verified"`
	Diagnostic      wireDiagnostic `json:"diagnostic"`
}

type wireDiagnostic struct {
	Action ec2vm.Action `json:"action"`
	Call   string       `json:"call"`
	Class  ec2api.Class `json:"class"`
	Sent   bool         `json:"sent"`
	Reason string       `json:"reason"`
}

// encodeState returns the canonical encoding of st: indented JSON with a
// final newline, an empty Reported as null and times in UTC.
func encodeState(st state) ([]byte, error) {
	r := st.rec
	reported := r.Reported
	if len(reported) == 0 {
		reported = nil
	}
	d := r.LastDiagnostic
	w := wireState{
		Format:  stateFormat,
		Version: stateVersion,
		Profile: toWireProfile(&st.profile),
		Restart: st.restart,
		Record: wireRecord{
			Owner:           r.Identity.Owner,
			Environment:     r.Identity.Environment,
			Generation:      r.Identity.Generation,
			Lease:           r.Identity.Lease,
			ProfileHash:     r.ProfileHash,
			Request:         r.Request,
			Token:           r.Token,
			Desired:         r.Desired,
			Phase:           r.Phase,
			Hold:            r.Hold,
			InstanceID:      r.InstanceID,
			Reported:        reported,
			CreateSent:      r.CreateSent,
			CreateSentAt:    r.CreateSentAt.UTC(),
			CreditsVerified: r.CreditsVerified,
			Diagnostic:      wireDiagnostic{Action: d.Action, Call: d.Call, Class: d.Class, Sent: d.Sent, Reason: d.Reason},
		},
	}
	data, err := json.MarshalIndent(w, "", "\t")
	if err != nil {
		return nil, fmt.Errorf("encode state: %w", err)
	}
	data = append(data, '\n')
	if len(data) > maxState {
		return nil, fmt.Errorf("encode state: %d bytes exceeds %d", len(data), maxState)
	}
	return data, nil
}

// decodeState decodes a state file. It refuses another format or version
// (errUnsupportedState), unknown fields, trailing data, a diagnostic this
// package could not have written, and any bytes encodeState would not
// produce for the decoded value. It checks only the encoding: see
// state.verify.
func decodeState(data []byte) (state, error) {
	var fence struct {
		Format  string `json:"format"`
		Version int    `json:"version"`
	}
	if err := json.Unmarshal(data, &fence); err != nil {
		return state{}, fmt.Errorf("decode state: %w", err)
	}
	if fence.Format != stateFormat || fence.Version != stateVersion {
		return state{}, fmt.Errorf("%w: %q version %d", errUnsupportedState, fence.Format, fence.Version)
	}
	var w wireState
	if err := decodeStrict(data, &w); err != nil {
		return state{}, fmt.Errorf("decode state: %w", err)
	}
	wr, wd := w.Record, w.Record.Diagnostic
	if wd.Action < ec2vm.ActionNone || wd.Action > ec2vm.ActionTerminate ||
		wd.Class < ec2api.ClassOther || wd.Class > ec2api.ClassMalformed ||
		!fixedText(wd.Call) || !fixedText(wd.Reason) {
		return state{}, errors.New("decode state: invalid diagnostic")
	}
	st := state{
		profile: w.Profile.profile(),
		restart: w.Restart,
		rec: ec2vm.Record{
			Identity:        ec2api.Identity{Owner: wr.Owner, Environment: wr.Environment, Generation: wr.Generation, Lease: wr.Lease},
			ProfileHash:     wr.ProfileHash,
			Request:         wr.Request,
			Token:           wr.Token,
			Desired:         wr.Desired,
			Phase:           wr.Phase,
			Hold:            wr.Hold,
			InstanceID:      wr.InstanceID,
			Reported:        wr.Reported,
			CreateSent:      wr.CreateSent,
			CreateSentAt:    wr.CreateSentAt,
			CreditsVerified: wr.CreditsVerified,
			LastDiagnostic:  ec2vm.Diagnostic{Action: wd.Action, Call: wd.Call, Class: wd.Class, Sent: wd.Sent, Reason: wd.Reason},
		},
	}
	again, err := encodeState(st)
	if err != nil {
		return state{}, err
	}
	if !bytes.Equal(again, data) {
		return state{}, errors.New("decode state: not in canonical form")
	}
	return st, nil
}

// verify checks st against the operator's configuration and returns it
// with its record as ec2vm.FromFields accepts it. Any disagreement with
// cfg is ErrMismatch; anything else wrong is a corrupt state.
func (st state) verify(cfg *Config) (state, error) {
	if err := st.profile.Validate(); err != nil {
		return state{}, fmt.Errorf("state profile: %w", err)
	}
	if st.profile.Hash() != cfg.Profile.Hash() {
		return state{}, fmt.Errorf("%w: profile", ErrMismatch)
	}
	if st.rec.Identity != cfg.Identity {
		return state{}, fmt.Errorf("%w: identity", ErrMismatch)
	}
	rec, err := ec2vm.FromFields(&cfg.Profile, st.rec)
	if err != nil {
		return state{}, fmt.Errorf("state record: %w", err)
	}
	req, err := ec2api.NewCreateRequest(&cfg.Profile, cfg.Identity, cfg.UserData)
	if err != nil {
		return state{}, err
	}
	if !bytes.Equal(rec.Request, req.Body()) {
		return state{}, fmt.Errorf("%w: create request (user data)", ErrMismatch)
	}
	if st.restart && rec.Desired != ec2vm.DesiredStopped {
		return state{}, errors.New("state wants a restart but its record does not want stopped")
	}
	st.rec = rec
	return st, nil
}

// fixedText reports whether s could be a diagnostic's fixed text: empty,
// or at most 64 printable ASCII bytes.
func fixedText(s string) bool {
	if len(s) > 64 {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < ' ' || s[i] > '~' {
			return false
		}
	}
	return true
}
