//go:build unix

package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/tmc/skygo/integrations/gcp"
	"golang.org/x/oauth2"
)

type oauthToken = oauth2.Token

const maxFileBytes = 16 << 10

// An inputCloseError is the failed first Close of an input file. It keeps
// the file; nothing closes it again, and the command stops before any
// later effect.
type inputCloseError struct {
	name string
	f    *os.File
	err  error
}

func (e *inputCloseError) Error() string { return "close " + e.name + ": " + e.err.Error() }
func (e *inputCloseError) Unwrap() error { return e.err }

// closeInput runs an input file's first Close. Tests replace it to inject
// a failure.
var closeInput = (*os.File).Close

// readPrivate reads the private regular file name: not a symbolic link,
// owned by the effective user, one link, no group or other access, at
// most 16 KiB. It succeeds only if the file's first Close does.
func readPrivate(name string) ([]byte, error) {
	f, err := os.OpenFile(name, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	b, err := readOpen(name, f)
	if cerr := closeInput(f); cerr != nil {
		return nil, errors.Join(err, &inputCloseError{name: name, f: f, err: cerr})
	}
	if err != nil {
		return nil, err
	}
	return b, nil
}

func readOpen(name string, f *os.File) ([]byte, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	switch {
	case !fi.Mode().IsRegular():
		return nil, fmt.Errorf("%s is not a regular file", name)
	case !ok || int(st.Uid) != os.Geteuid():
		return nil, fmt.Errorf("%s is not owned by this user", name)
	case fi.Mode().Perm()&0o077 != 0:
		return nil, fmt.Errorf("%s has mode %v; group and other must have no access", name, fi.Mode().Perm())
	case st.Nlink != 1:
		return nil, fmt.Errorf("%s has %d links, want 1", name, st.Nlink)
	}
	b, err := io.ReadAll(io.LimitReader(f, maxFileBytes+1))
	if err != nil {
		return nil, err
	}
	if len(b) > maxFileBytes {
		return nil, fmt.Errorf("%s exceeds %d bytes", name, maxFileBytes)
	}
	return b, nil
}

func readConfig(name string) (gce.Config, error) {
	b, err := readPrivate(name)
	if err != nil {
		return gce.Config{}, fmt.Errorf("config: %w", err)
	}
	return gce.ParseConfig(bytes.NewReader(b))
}

// tokenFile is the only accepted credential form: an access token, its
// type and its expiry. Refresh tokens, client secrets and service account
// keys are unknown fields and refused.
type tokenFile struct {
	AccessToken string    `json:"access_token"`
	TokenType   string    `json:"token_type"`
	Expiry      time.Time `json:"expiry"`
}

// tokenKeys are the exact spellings of the token file's fields.
var tokenKeys = map[string]bool{"access_token": true, "token_type": true, "expiry": true}

// checkTokenKeys reports whether b is one JSON object whose decoded keys
// are each exactly one of tokenKeys, at most once. Keys are compared after
// JSON unescaping, and also case-folded, since encoding/json would match
// a case-varied key to the same field. Its errors never include values.
func checkTokenKeys(b []byte) error {
	dec := json.NewDecoder(bytes.NewReader(b))
	if t, err := dec.Token(); err != nil || t != json.Delim('{') {
		return errors.New("credentials: not a JSON object")
	}
	seen := map[string]bool{}
	for dec.More() {
		t, err := dec.Token()
		k, ok := t.(string)
		if err != nil || !ok {
			return errors.New("credentials: malformed JSON")
		}
		fold := strings.ToLower(strings.ToUpper(k))
		switch {
		case !tokenKeys[k]:
			return errors.New("credentials: not a token file with access_token, token_type and expiry")
		case seen[fold]:
			return fmt.Errorf("credentials: duplicate %s", k)
		}
		seen[fold] = true
		v, err := dec.Token() // all three values are strings
		if _, ok := v.(string); err != nil || !ok {
			return errors.New("credentials: field values must be strings")
		}
	}
	if t, err := dec.Token(); err != nil || t != json.Delim('}') {
		return errors.New("credentials: malformed JSON")
	}
	return nil
}

// readToken reads the private token file name. Its errors never include
// the token.
func readToken(name string) (oauth2.Token, error) {
	b, err := readPrivate(name)
	if err != nil {
		return oauth2.Token{}, fmt.Errorf("credentials: %w", err)
	}
	if err := checkTokenKeys(b); err != nil {
		return oauth2.Token{}, err
	}
	var t tokenFile
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&t); err != nil {
		return oauth2.Token{}, errors.New("credentials: not a token file with access_token, token_type and expiry")
	}
	if _, err := dec.Token(); err != io.EOF {
		return oauth2.Token{}, errors.New("credentials: trailing data")
	}
	if t.AccessToken == "" || !strings.EqualFold(t.TokenType, "bearer") || t.Expiry.IsZero() {
		return oauth2.Token{}, errors.New("credentials: need a bearer access_token and expiry")
	}
	return oauth2.Token{AccessToken: t.AccessToken, TokenType: "Bearer", Expiry: t.Expiry}, nil
}

// report is the version 1 JSON form of a status.
type report struct {
	Version          int       `json:"version"`
	Project          string    `json:"project"`
	ComputeProjectID string    `json:"compute_project_id"`
	Zone             string    `json:"zone"`
	InstanceName     string    `json:"instance_name"`
	InstanceID       string    `json:"instance_id"`
	InstanceState    string    `json:"instance_state"`
	DiskName         string    `json:"disk_name"`
	DiskID           string    `json:"disk_id"`
	DiskState        string    `json:"disk_state"`
	ImageID          string    `json:"image_id"`
	Desired          string    `json:"desired"`
	PendingRequestID string    `json:"pending_request_id"`
	PendingOperation string    `json:"pending_operation"`
	ObservedAt       time.Time `json:"observed_at"`
	Reached          bool      `json:"reached"`
	Held             string    `json:"held"`
}

func printStatus(w io.Writer, asJSON bool, st gce.Status) error {
	r := report{1, st.Project, st.ComputeProjectID, st.Zone, st.InstanceName, st.InstanceID, st.InstanceState,
		st.DiskName, st.DiskID, st.DiskState, st.ImageID, string(st.Desired), st.PendingRequestID, st.PendingOperation,
		st.ObservedAt.UTC(), st.Reached, st.Held}
	if asJSON {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(r)
	}
	var b strings.Builder
	line := func(k, v string) {
		if v == "" {
			v = "-"
		}
		fmt.Fprintf(&b, "%-17s %s\n", k, v)
	}
	line("project", r.Project+" ("+r.ComputeProjectID+")")
	line("zone", r.Zone)
	line("instance", r.InstanceName+" "+r.InstanceID)
	line("instance state", r.InstanceState)
	line("disk", r.DiskName+" "+r.DiskID)
	line("disk state", r.DiskState)
	line("image id", r.ImageID)
	line("desired", r.Desired)
	line("pending request", r.PendingRequestID)
	line("pending operation", r.PendingOperation)
	line("held", r.Held)
	line("goal reached", fmt.Sprint(r.Reached))
	_, err := io.WriteString(w, b.String())
	return err
}
