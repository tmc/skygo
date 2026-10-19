package wire

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"unicode/utf8"

	"github.com/tmc/skygo/integrations/skycompat/internal/pickleemit"
)

// APIVersion identifies the upstream wire profile, not a server capability claim.
const (
	APIVersion       = 67
	APIVersionHeader = "X-SkyPilot-API-Version"
	VersionHeader    = "X-SkyPilot-Version"
	RequestIDHeader  = "X-Skypilot-Request-ID"
)

// ExceptionType is one of the two builtin exception types this codec emits.
type ExceptionType string

const (
	ValueError   ExceptionType = "ValueError"
	RuntimeError ExceptionType = "RuntimeError"
)

// Exception contains an allowlisted type and a server-controlled, safe message.
// Message must be valid UTF-8 of at most 4096 bytes. Do not put credentials,
// client-supplied field names, or arbitrary error strings in Message.
type Exception struct {
	Type    ExceptionType
	Message string
}

// RequestStatus describes an API operation, not the workload it may submit.
type RequestStatus string

const (
	Pending   RequestStatus = "PENDING"
	Waiting   RequestStatus = "WAITING"
	Running   RequestStatus = "RUNNING"
	Succeeded RequestStatus = "SUCCEEDED"
	Failed    RequestStatus = "FAILED"
	Cancelled RequestStatus = "CANCELLED"
)

// ScheduleType identifies the upstream request queue.
type ScheduleType string

const (
	Long  ScheduleType = "long"
	Short ScheduleType = "short"
)

// Request is server-selected operation metadata. Its zero value is invalid.
// IDs and Name are nonempty UTF-8 strings of at most 256 bytes. Timestamps are
// finite, nonnegative Unix seconds. Optional text is limited to 4096 bytes.
// Result is JSON data, not a Go object graph; empty means null. It is limited
// to 256 KiB, depth 32, 65536 tokens, and 64 KiB per string. The codec does not
// establish ownership or verify the operation's domain-specific result shape.
type Request struct {
	ID, Name, UserID           string
	Status                     RequestStatus
	CreatedAt                  float64
	Schedule                   ScheduleType
	ClusterName, StatusMessage *string
	FinishedAt                 *float64
	Result                     json.RawMessage
}

type requestPayload struct {
	ID               string        `json:"request_id"`
	Name             string        `json:"name"`
	Entrypoint       string        `json:"entrypoint"`
	Body             string        `json:"request_body"`
	Status           RequestStatus `json:"status"`
	CreatedAt        float64       `json:"created_at"`
	UserID           string        `json:"user_id"`
	Result           string        `json:"return_value"`
	Error            string        `json:"error"`
	PID              *int          `json:"pid"`
	Schedule         ScheduleType  `json:"schedule_type"`
	UserName         *string       `json:"user_name"`
	ClusterName      *string       `json:"cluster_name"`
	StatusMessage    *string       `json:"status_msg"`
	ShouldRetry      bool          `json:"should_retry"`
	FinishedAt       *float64      `json:"finished_at"`
	FileMountsBlobID *string       `json:"file_mounts_blob_id"`
}

// EncodeRequest writes the JSON body for a nonfailed API operation response.
// It fills bookkeeping pickle fields with None and string-encodes Result.
// Failed operations must use EncodeRequestError. This does not write HTTP
// headers or status. Validation and the 1 MiB response bound precede all writes.
func EncodeRequest(w io.Writer, r Request) error {
	if r.Status == Failed {
		return errors.New("wire: failed request requires an exception")
	}
	return encodeRequest(w, r)
}
func encodeRequest(w io.Writer, r Request) error {
	p, err := payload(r)
	if err != nil {
		return err
	}
	return writeJSON(w, p)
}

func payload(r Request) (requestPayload, error) {
	for _, s := range []string{r.ID, r.Name, r.UserID} {
		if s == "" || len(s) > 256 || !utf8.ValidString(s) {
			return requestPayload{}, errors.New("wire: invalid request identity")
		}
	}
	switch r.Status {
	case Pending, Waiting, Running, Succeeded, Failed, Cancelled:
	default:
		return requestPayload{}, errors.New("wire: invalid request status")
	}
	if r.Schedule != Long && r.Schedule != Short {
		return requestPayload{}, errors.New("wire: invalid request schedule")
	}
	if !timestamp(r.CreatedAt) || (r.FinishedAt != nil && !timestamp(*r.FinishedAt)) {
		return requestPayload{}, errors.New("wire: invalid request timestamp")
	}
	for _, s := range []*string{r.ClusterName, r.StatusMessage} {
		if s != nil && (len(*s) > 4096 || !utf8.ValidString(*s)) {
			return requestPayload{}, errors.New("wire: invalid request text")
		}
	}
	result := r.Result
	if len(result) == 0 {
		result = json.RawMessage("null")
	}
	if len(result) > maxResult {
		return requestPayload{}, errors.New("wire: result exceeds limit")
	}
	if err := checkJSON(result, 64<<10); err != nil {
		return requestPayload{}, errors.New("wire: invalid result JSON")
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, result); err != nil {
		return requestPayload{}, errors.New("wire: invalid result JSON")
	}
	return requestPayload{
		ID: r.ID, Name: r.Name, Entrypoint: "gAROLg==", Body: "gAROLg==",
		Status: r.Status, CreatedAt: r.CreatedAt, UserID: r.UserID,
		Result: compact.String(), Error: "null", Schedule: r.Schedule,
		ClusterName: r.ClusterName, StatusMessage: r.StatusMessage, FinishedAt: r.FinishedAt,
	}, nil
}
func timestamp(t float64) bool { return t >= 0 && !math.IsNaN(t) && !math.IsInf(t, 0) }

// EncodeRequestError writes the detail-wrapped RequestPayload body for HTTP500.
// The caller supplies a Failed request with no result (empty or null) and an
// allowlisted Exception. The error pickle contains only primitive data; no
// client-supplied object, executable opcode, stack trace, or arbitrary class.
func EncodeRequestError(w io.Writer, r Request, e Exception) error {
	if r.Status != Failed || (len(r.Result) != 0 && !isNull(r.Result)) {
		return errors.New("wire: exception requires failed request without a result")
	}
	return encodeRequestError(w, r, e)
}
func encodeRequestError(w io.Writer, r Request, e Exception) error {
	p, err := payload(r)
	if err != nil {
		return err
	}
	data, err := pickleemit.Exception(string(e.Type), e.Message)
	if err != nil {
		return fmt.Errorf("wire: %w", err)
	}
	errorJSON, err := json.Marshal(struct {
		Object  string        `json:"object"`
		Type    ExceptionType `json:"type"`
		Message string        `json:"message"`
	}{base64.StdEncoding.EncodeToString(data), e.Type, e.Message})
	if err != nil {
		return err
	}
	p.Error = string(errorJSON)
	return writeJSON(w, struct {
		Detail requestPayload `json:"detail"`
	}{p})
}

// EncodeValidationError writes the plain exception detail body for HTTP400.
// Unlike EncodeRequestError, this representation contains no request envelope
// or pickle. It does not write HTTP headers or a status code.
func EncodeValidationError(w io.Writer, e Exception) error {
	if e.Type != RuntimeError && e.Type != ValueError {
		return errors.New("wire: unsupported exception type")
	}
	if len(e.Message) > 4096 || !utf8.ValidString(e.Message) {
		return errors.New("wire: invalid exception message")
	}
	return encodeValidationError(w, e)
}

type exceptionPayload struct {
	Type       ExceptionType `json:"type"`
	Message    string        `json:"message"`
	Args       []string      `json:"args"`
	Attributes struct{}      `json:"attributes"`
	Stacktrace *string       `json:"stacktrace"`
}

func encodeValidationError(w io.Writer, e Exception) error {
	return writeJSON(w, struct {
		Detail exceptionPayload `json:"detail"`
	}{
		Detail: exceptionPayload{Type: e.Type, Message: e.Message, Args: []string{e.Message}},
	})
}

func writeJSON(w io.Writer, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(data) > maxBody {
		return errors.New("wire: response exceeds limit")
	}
	if w == nil {
		return errors.New("wire: nil response writer")
	}
	n, err := w.Write(data)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	if err != nil {
		return fmt.Errorf("write wire response: %w", err)
	}
	return nil
}
