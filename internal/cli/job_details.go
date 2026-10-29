package cli

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	skyv1 "github.com/tmc/skygo/gen/skygo/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Human details show at most the latest eight attempts and 512 bytes of any
// server string. JSON keeps the complete original protobuf representation.
const jobDetailTextLimit = 512
const jobDetailAttemptLimit = 8

func jobDetailText(s string) string {
	if len(s) <= jobDetailTextLimit {
		return strconv.QuoteToASCII(s)
	}
	n := jobDetailTextLimit
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return strconv.QuoteToASCII(s[:n]) + " (truncated)"
}

func (o *options) jobDetails(w io.Writer, j *skyv1.Job) error {
	if j == nil {
		return errors.New("server returned no job")
	}
	if o.json {
		return writeJSON(w, j)
	}
	// Accumulate before writing so every writer failure propagates unchanged.
	var b strings.Builder
	fmt.Fprintf(&b, "ID: %s\nTarget: %s\nState: %s\n", jobDetailText(j.Id), jobDetailText(j.Target), state(j.State))
	if j.Reason != "" {
		fmt.Fprintf(&b, "Reason: %s\n", jobDetailText(j.Reason))
	}
	if j.ExitCode != nil {
		fmt.Fprintf(&b, "Exit code: %d\n", *j.ExitCode)
	}
	jobDetailTime(&b, "Created (recorded)", j.CreatedAt)
	jobDetailTime(&b, "Updated (recorded)", j.UpdatedAt)
	jobDetailTime(&b, "Next attempt (scheduled)", j.NextAttemptAt)
	start := max(0, len(j.Attempts)-jobDetailAttemptLimit)
	if start > 0 {
		fmt.Fprintf(&b, "Attempts: %d earlier omitted\n", start)
	}
	for _, a := range j.Attempts[start:] {
		if a == nil {
			continue
		}
		fmt.Fprintf(&b, "Attempt %d: %s %s\n", a.Ordinal, jobDetailText(a.Id), state(a.State))
		if a.Reason != "" {
			fmt.Fprintf(&b, "  Reason: %s\n", jobDetailText(a.Reason))
		}
		if a.ExitCode != nil {
			fmt.Fprintf(&b, "  Exit code: %d\n", *a.ExitCode)
		}
		jobDetailTime(&b, "  Created (recorded)", a.CreatedAt)
		jobDetailTime(&b, "  Starting (recorded)", a.StartingAt)
		jobDetailTime(&b, "  Finished (recorded)", a.FinishedAt)
	}
	n, err := io.WriteString(w, b.String())
	if err == nil && n != b.Len() {
		err = io.ErrShortWrite
	}
	return err
}

func jobDetailTime(w io.Writer, label string, ts *timestamppb.Timestamp) {
	if ts != nil && ts.CheckValid() == nil {
		fmt.Fprintf(w, "%s: %s\n", label, ts.AsTime().UTC().Format(time.RFC3339Nano))
	}
}
