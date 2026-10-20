package rpc

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync"

	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/control"
)

// HTTPHandler owns admitted artifact HTTP requests through positive completion.
// It implements http.Handler. The zero value is unusable; use NewHandler.
// An uncertain original request-body Close retains its concrete body, response,
// error and Control call indefinitely rather than manufacturing a join.
type HTTPHandler struct {
	serve          http.Handler
	mu             sync.Mutex
	owners         map[*artifactResponse]struct{}
	stopped        bool
	terminal       chan struct{}
	terminalClosed bool
}

func (h *HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.serve.ServeHTTP(w, r)
}

// WaitArtifactHTTP atomically stops new artifact HTTP admission and joins only
// the actual admitted owners. A blocked or uncertain body Close prevents return.
// This neither retries Close nor proves closure of unadmitted network requests,
// Control downloads, backend workers or borrowed Files/root descriptors.
func (h *HTTPHandler) WaitArtifactHTTP() {
	<-h.stopArtifactHTTP()
}

func (h *HTTPHandler) stopArtifactHTTP() <-chan struct{} {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.stopped = true
	h.completeLocked()
	return h.terminal
}

func (h *HTTPHandler) completeLocked() {
	if h.stopped && len(h.owners) == 0 && !h.terminalClosed {
		h.terminalClosed = true
		close(h.terminal)
	}
}

func (h *HTTPHandler) admit(c *control.Control, body io.ReadCloser, writer http.ResponseWriter) (*artifactResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.stopped {
		return nil, control.ErrUnavailable
	}
	// The retained original responses have their own concrete bound. A free
	// Control permit alone never authorizes a fifth still-mapped body/writer.
	if len(h.owners) >= 4 {
		return nil, control.ErrArtifactBusy
	}
	// This try-acquires the same four actual Control slots before Connect
	// decoding, including metadata-only methods. No second owner budget exists.
	call, err := c.TryArtifactCall()
	if err != nil {
		return nil, err
	}
	owner := &artifactResponse{handler: h, call: call, body: &artifactRequestBody{original: body}, writer: writer}
	h.owners[owner] = struct{}{}
	return owner, nil
}

type artifactResponseKey struct{}

// artifactRequestBody caches the admitted incoming body's Close result for
// calls through this wrapper, including Connect and the enclosing finisher.
// It never retries that owner. net/http separately saved its transport body
// before handler entry and may close it directly; standard http.body.Close is
// internally idempotent. That later transport Close cannot clear our retained
// error. This wrapper claims no interception of net/http's saved reference.
// Read errors alone do not imply Close uncertainty.
type artifactRequestBody struct {
	original io.ReadCloser
	once     sync.Once
	err      error
}

func (b *artifactRequestBody) Read(data []byte) (int, error) {
	if b.original == nil {
		return 0, io.EOF
	}
	return b.original.Read(data)
}

func (b *artifactRequestBody) Close() error {
	b.once.Do(func() {
		if b.original != nil {
			b.err = b.original.Close()
		}
	})
	return b.err
}

// artifactResponse is retained by the concrete handler map from admission until
// positive body closure and original synchronous response completion. The map is
// bounded by the four admitted calls, not by an unbounded error queue.
type artifactResponse struct {
	handler    *HTTPHandler
	call       *control.ArtifactCall
	body       *artifactRequestBody
	writer     http.ResponseWriter
	finishOnce sync.Once
	used       bool
	closeErr   error
	flushErr   error
}

func (r *artifactResponse) finish() {
	r.finishOnce.Do(func() {
		r.closeErr = r.body.Close()
		if r.closeErr != nil {
			// Keep the actual body/error/response/call alive after handler return.
			// There is no same-owner recovery receipt and no permitted raw retry.
			return
		}
		// A returned write/flush error is recorded separately. With actual nil
		// body Close and returned synchronous writes, it does not fabricate an
		// unknown body owner or prevent ordinary response-error recovery.
		r.flushErr = http.NewResponseController(r.writer).Flush()
		h := r.handler
		h.mu.Lock()
		// Match admit's HTTP mutex -> Control mutex order. Done is bounded
		// in-memory ownership bookkeeping, with no I/O or Store callbacks.
		// Permit release and map retirement are one admission boundary.
		r.call.Done()
		delete(h.owners, r)
		h.completeLocked()
		h.mu.Unlock()
	})
}

func artifactProcedure(procedure string) bool {
	switch procedure {
	case skyv1connect.ControlServiceBeginArtifactUploadProcedure,
		skyv1connect.ControlServiceGetArtifactUploadProcedure,
		skyv1connect.ControlServiceWriteArtifactChunkProcedure,
		skyv1connect.ControlServiceFinalizeArtifactUploadProcedure,
		skyv1connect.ControlServiceCancelArtifactUploadProcedure,
		skyv1connect.ControlServiceBeginArtifactDownloadProcedure,
		skyv1connect.ControlServiceReadArtifactChunkProcedure,
		skyv1connect.ControlServiceCloseArtifactDownloadProcedure,
		skyv1connect.ControlServiceRemoveArtifactProcedure,
		skyv1connect.ControlServiceSubmitWorkspaceJobProcedure:
		return true
	}
	return false
}

func (h *handler) artifactActor(ctx context.Context, action auth.Action) (auth.Actor, error) {
	actor, err := h.engine.Check(requestActor(ctx), action)
	if err != nil {
		return auth.Actor{}, err
	}
	if err := ctx.Err(); err != nil {
		return auth.Actor{}, err
	}
	return actor, nil
}

func (h *handler) artifactCall(ctx context.Context, action auth.Action) (*control.ArtifactCall, error) {
	if _, err := h.artifactActor(ctx, action); err != nil {
		return nil, err
	}
	response, ok := ctx.Value(artifactResponseKey{}).(*artifactResponse)
	if !ok || response == nil || response.call == nil || response.used {
		return nil, errors.New("artifact HTTP owner unavailable")
	}
	response.used = true
	return response.call, nil
}

// artifactWriter rechecks current authority at each actual response write. A
// failed or revoked publication does not undo admitted filesystem effects or
// release the original HTTP owner early.
type artifactWriter struct {
	http.ResponseWriter
	ctx     context.Context
	handler *handler
	action  auth.Action
}

func (w *artifactWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *artifactWriter) Write(data []byte) (int, error) {
	if _, err := w.handler.artifactActor(w.ctx, w.action); err != nil {
		return 0, err
	}
	return w.ResponseWriter.Write(data)
}

func (w *artifactWriter) FlushError() error {
	if _, err := w.handler.artifactActor(w.ctx, w.action); err != nil {
		return err
	}
	return http.NewResponseController(w.ResponseWriter).Flush()
}

func artifactAction(procedure string) auth.Action {
	switch procedure {
	case skyv1connect.ControlServiceGetArtifactUploadProcedure,
		skyv1connect.ControlServiceBeginArtifactDownloadProcedure,
		skyv1connect.ControlServiceReadArtifactChunkProcedure,
		skyv1connect.ControlServiceCloseArtifactDownloadProcedure:
		return auth.Read
	}
	return auth.Mutate
}
