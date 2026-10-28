package kube

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"github.com/tmc/skygo/internal/backend"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/remotecommand"
	"k8s.io/apimachinery/pkg/util/validation"
)

// Environment exec runs one noninteractive command over the v4 remotecommand
// protocol on one frame. It creates the protocol's streams in the order of
// client-go's streamProtocolV2.createStreams (error, stdin, stdout, stderr)
// and starts only workers it owns and joins: an optional stdin sender, the
// stdout and stderr readers and the status reader. client-go's own streamer is
// not used because its stdin copiers are not joined and it discards their
// errors (tools/remotecommand/v2.go in v0.32.3).
//
// A reported exit is the command's status as the API server relayed it. It is
// not proof that descendants of the command stopped. A missing, malformed,
// truncated or oversized status is Unknown, never exit zero. Cancellation
// closes the connection; it never reruns the command or resends stdin.

const (
	maxExecArgv   = 64 << 10 // total argv bytes
	maxExecStdin  = 64 << 10
	maxExecStatus = 64 << 10
	// maxExecBody bounds a streamed stdin body.
	maxExecBody = maxEnvironmentArtifactBytes
)

// errExecStatusLimit reports a status stream longer than maxExecStatus. The
// exit is Unknown.
var errExecStatusLimit = errors.New("kubernetes exec status exceeds 64 KiB")

// execRequest is a noninteractive command for one container of one Pod. The
// caller has already checked the environment, session, Pod and container.
type execRequest struct {
	Pod       string
	Container string
	Argv      []string // passed as argv; no shell expansion
	Stdin     []byte   // nil sends no stdin stream
	// Body, if not nil, is sent as stdin in place of Stdin, which must then
	// be nil: exactly BodySize bytes of it, read by one goroutine of the
	// frame, and then the end of stdin. The frame never reads Body after
	// it settles.
	Body     io.Reader
	BodySize int64
}

// execResult is a snapshot of an exec frame.
type execResult struct {
	envState
	// Reported is set when the status stream carried a valid v4 status with
	// an exit code; Code is that code.
	Reported bool
	Code     int
	// Stdout and Stderr are the retained output. Their combined length is at
	// most backend.MaxLogBytes; Truncated reports that more was read and
	// discarded.
	Stdout, Stderr []byte
	Truncated      bool
	// BodySent reports that exactly the request's BodySize bytes of Body
	// were written to stdin and stdin was then closed.
	BodySent bool
}

// execFrame is the frame of one environment exec.
type execFrame struct {
	*envFrame

	out       sync.Mutex // guards the fields below
	stdout    []byte
	stderr    []byte
	truncated bool
	reported  bool
	code      int
	bodySent  bool
}

// execURL validates r and returns its fixed pods/exec URL.
func (t *envTransport) execURL(r execRequest) (*url.URL, error) {
	if len(validation.IsDNS1123Label(r.Container)) != 0 {
		return nil, errors.New("invalid kubernetes container name")
	}
	if len(r.Argv) == 0 {
		return nil, errors.New("kubernetes exec requires argv")
	}
	n := 0
	for _, a := range r.Argv {
		if strings.IndexByte(a, 0) >= 0 {
			return nil, errors.New("kubernetes exec argv contains NUL")
		}
		n += len(a)
	}
	if n > maxExecArgv {
		return nil, errors.New("kubernetes exec argv exceeds 64 KiB")
	}
	if len(r.Stdin) > maxExecStdin {
		return nil, errors.New("kubernetes exec stdin exceeds 64 KiB")
	}
	if r.Body != nil && (r.Stdin != nil || r.BodySize < 0 || r.BodySize > maxExecBody) {
		return nil, errors.New("invalid kubernetes exec stdin body")
	}
	q := url.Values{
		"container": {r.Container},
		"command":   r.Argv,
		"stdout":    {"true"},
		"stderr":    {"true"},
	}
	if r.Stdin != nil || r.Body != nil {
		q.Set("stdin", "true")
	}
	return t.podURL(r.Pod, "exec", q)
}

// startExec validates r, then opens its frame and starts the exec in an owned
// goroutine. The frame is the only record of the original call.
func (t *envTransport) startExec(r execRequest) (*execFrame, error) {
	u, err := t.execURL(r)
	if err != nil {
		return nil, err
	}
	in := execInput{body: r.Body, size: r.BodySize}
	if r.Stdin != nil {
		in.data = append([]byte{}, r.Stdin...)
	}
	x := &execFrame{envFrame: newEnvFrame()}
	x.spawn(func() error { return x.run(t, u, in) })
	return x, nil
}

// wait returns the frame's result once it settles. If ctx ends first, wait
// cancels the frame and returns its current, unsettled result; the original
// calls continue under the frame, and a later wait observes them. A settled
// frame is never canceled, even if ctx has already ended.
func (x *execFrame) wait(ctx context.Context) execResult {
	select {
	case <-x.done:
		return x.result()
	default:
	}
	select {
	case <-x.done:
	case <-ctx.Done():
		x.cancel()
	}
	return x.result()
}

func (x *execFrame) result() execResult {
	s := x.state()
	x.out.Lock()
	defer x.out.Unlock()
	return execResult{envState: s, Reported: x.reported, Code: x.code,
		Stdout: append([]byte(nil), x.stdout...), Stderr: append([]byte(nil), x.stderr...), Truncated: x.truncated,
		BodySent: x.bodySent}
}

// execInput is the stdin of one exec: data, or size bytes of body. Neither
// set sends no stdin stream.
type execInput struct {
	data []byte
	body io.Reader
	size int64
}

// run is the exec's supervisor. It returns after every worker it started has
// returned and the connection's Close, if a connection was published, has
// returned, together with the original connection's Close that it waits for.
func (x *execFrame) run(t *envTransport, u *url.URL, stdin execInput) error {
	conn, err := x.dial(t, u, execProtocol)
	if err != nil {
		return err
	}
	abort := func(err error) error {
		if c := x.closeConn(); c != nil {
			<-c
		}
		return err
	}
	h := http.Header{}
	h.Set(corev1.StreamType, corev1.StreamTypeError)
	status, err := x.createStream(conn, "error", h)
	if err != nil {
		return abort(err)
	}
	var in *envStream
	if stdin.data != nil || stdin.body != nil {
		h.Set(corev1.StreamType, corev1.StreamTypeStdin)
		if in, err = x.createStream(conn, "stdin", h); err != nil {
			return abort(err)
		}
	}
	h.Set(corev1.StreamType, corev1.StreamTypeStdout)
	stdout, err := x.createStream(conn, "stdout", h)
	if err != nil {
		return abort(err)
	}
	h.Set(corev1.StreamType, corev1.StreamTypeStderr)
	stderr, err := x.createStream(conn, "stderr", h)
	if err != nil {
		return abort(err)
	}

	workers := []<-chan struct{}{
		x.spawn(func() error { return x.readStatus(status) }),
		x.spawn(func() error { return x.readOutput(stdout, &x.stdout) }),
		x.spawn(func() error { return x.readOutput(stderr, &x.stderr) }),
	}
	switch {
	case in != nil && stdin.body != nil:
		workers = append(workers, x.spawn(func() error { return x.sendBody(in, stdin.body, stdin.size) }))
	case in != nil:
		workers = append(workers, x.spawn(func() error { return x.sendStdin(in, stdin.data) }))
	}
	for _, w := range workers {
		<-w
	}
	// Every stream ended without cancellation or failure: reset each one
	// explicitly before closing the connection. Otherwise the connection is
	// already closing.
	if !x.isClosing() {
		x.resetAll()
	}
	if c := x.closeConn(); c != nil {
		<-c
	}
	return nil
}

// fail closes the connection to release the other workers and returns err.
// It does not wait for the Close.
func (x *execFrame) fail(err error) error {
	x.closeConn()
	return err
}

func (x *execFrame) sendStdin(s *envStream, data []byte) error {
	if len(data) > 0 {
		if _, err := s.Write(data); err != nil {
			return x.fail(fmt.Errorf("write kubernetes exec stdin: %w", err))
		}
	}
	if err := s.closeWrite(); err != nil {
		return x.fail(err)
	}
	return nil
}

// sendBody copies exactly size bytes of body to s and then closes s for
// writing. A body that ends early or fails is an error, and stdin is not
// closed: the frame's connection is closed instead, so the command never
// sees a normal end of a short input.
func (x *execFrame) sendBody(s *envStream, body io.Reader, size int64) error {
	if _, err := io.CopyN(s, body, size); err != nil {
		return x.fail(fmt.Errorf("stream kubernetes exec stdin: %w", err))
	}
	if err := s.closeWrite(); err != nil {
		return x.fail(err)
	}
	x.out.Lock()
	x.bodySent = true
	x.out.Unlock()
	return nil
}

// readOutput reads s to its end, retaining output while the combined
// retained output is within backend.MaxLogBytes and discarding the rest.
func (x *execFrame) readOutput(s *envStream, dst *[]byte) error {
	buf := make([]byte, 32<<10)
	for {
		n, err := s.Read(buf)
		if n > 0 {
			x.out.Lock()
			room := backend.MaxLogBytes - len(x.stdout) - len(x.stderr)
			if n > room {
				x.truncated = true
				n = max(room, 0)
			}
			*dst = append(*dst, buf[:n]...)
			x.out.Unlock()
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return x.fail(fmt.Errorf("read kubernetes exec %s: %w", s.name, err))
		}
	}
}

// readStatus reads the bounded status stream and records a reported exit.
func (x *execFrame) readStatus(s *envStream) error {
	b, err := io.ReadAll(io.LimitReader(s, maxExecStatus+1))
	if err != nil {
		return x.fail(fmt.Errorf("read kubernetes exec status: %w", err))
	}
	if len(b) > maxExecStatus {
		return x.fail(errExecStatusLimit)
	}
	code, err := execStatus(b)
	if err != nil {
		return err
	}
	x.out.Lock()
	x.reported, x.code = true, code
	x.out.Unlock()
	return nil
}

// execStatus decodes a v4 status as client-go's errorDecoderV4 does and
// returns the reported exit code. Anything else is an error: the exit is
// Unknown.
func execStatus(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, errors.New("kubernetes exec status missing")
	}
	var st metav1.Status
	if err := json.Unmarshal(b, &st); err != nil {
		return 0, fmt.Errorf("kubernetes exec status malformed: %w", err)
	}
	switch {
	case st.Status == metav1.StatusSuccess:
		return 0, nil
	case st.Status == metav1.StatusFailure && st.Reason == remotecommand.NonZeroExitCodeReason:
		if st.Details == nil {
			return 0, errors.New("kubernetes exec status has no details")
		}
		for _, c := range st.Details.Causes {
			if c.Type != remotecommand.ExitCodeCauseType {
				continue
			}
			rc, err := strconv.ParseUint(c.Message, 10, 8)
			if err != nil {
				return 0, fmt.Errorf("kubernetes exec status has invalid exit code %q", c.Message)
			}
			return int(rc), nil
		}
		return 0, errors.New("kubernetes exec status has no exit code")
	case st.Status == metav1.StatusFailure:
		return 0, fmt.Errorf("kubernetes exec failed: %s", st.Message)
	}
	return 0, errors.New("kubernetes exec status unknown")
}
