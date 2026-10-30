package kube

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"

	"github.com/tmc/skygo/internal/backend"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
)

// errWorkerLogsStatus reports a log response with a non-2xx status.
var errWorkerLogsStatus = errors.New("kubernetes worker log request failed")

// WorkerLogs reads the retained prefix of the member's task container. The
// log stream is addressed by name, so the chunk counts only if the original
// UID is still at the name after the read.
//
// Complete means only that the chunk ends the retained prefix this read
// returned: either the member has positive stop evidence, or the prefix was
// truncated at backend.MaxLogBytes, in which case the worker may still be
// running. Complete is never termination. Offsets index this read's retained
// prefix only; they are no cursor into, and no authority over, the bytes the
// container wrote.
//
// The read shares one bounded member frame with the rank's other operations.
// Its actual request and response bodies close once. An unsettled result
// retains the original frame, bodies and causes through ErrWorkerUnsettled;
// context cancellation releases nothing. Retirement excludes this original
// read until its actual return and positive body closure. No body Close is
// retried and no transport error is interpreted as proof of termination.
//
// No error, end of stream or byte limit is stop evidence or shows that a
// connection closed.
func (rt *workerRuntime) WorkerLogs(ctx context.Context, ref backend.WorkerReference, offset int64, limit int) (chunk backend.LogChunk, err error) {
	if offset < 0 || limit <= 0 || limit > backend.MaxChunkBytes {
		return backend.LogChunk{}, fmt.Errorf("invalid log offset or limit: %w", fs.ErrInvalid)
	}
	m, err := rt.member(ref, false)
	if err != nil {
		return backend.LogChunk{}, err
	}
	key := workerCallKey(ref.Group, ref.Rank)
	ctx, call, err := rt.http.begin(ctx, key, false, true)
	if err != nil {
		return backend.LogChunk{}, err
	}
	defer func() {
		err = call.finish(err)
		if errors.Is(err, backend.ErrWorkerUnsettled) {
			chunk = backend.LogChunk{}
		}
	}()
	got, _, err := rt.read(ctx, m)
	if errors.Is(err, backend.ErrWorkerUnsettled) {
		return backend.LogChunk{}, err
	}
	if errors.Is(err, backend.ErrWorkerVanished) {
		return backend.LogChunk{}, fmt.Errorf("worker logs are no longer retained: %w: %w", fs.ErrNotExist, err)
	}
	if err != nil {
		return backend.LogChunk{}, err
	}
	stop, _ := workerStop(got)
	if s := workerTaskStatus(got); s == nil || s.ContainerID == "" {
		// The task container never ran, or has not yet.
		if offset != 0 {
			return backend.LogChunk{}, fmt.Errorf("log offset exceeds retained prefix: %w", fs.ErrInvalid)
		}
		return backend.LogChunk{Complete: stop != backend.WorkerStopUnproven}, nil
	}
	resp, err := rt.openWorkerLogs(ctx, got.Name)
	if err != nil {
		return backend.LogChunk{}, err
	}
	data, err := workerReadLogs(resp)
	err = errors.Join(err, rt.http.check(ctx))
	if err != nil {
		return backend.LogChunk{}, err
	}
	after, err := rt.getPod(ctx, got.Name)
	if err != nil {
		return backend.LogChunk{}, err
	}
	if after.UID != got.UID {
		return backend.LogChunk{}, errWorkerPodChanged
	}
	truncated := len(data) > backend.MaxLogBytes
	if truncated {
		data = data[:backend.MaxLogBytes]
	}
	if offset > int64(len(data)) {
		return backend.LogChunk{}, fmt.Errorf("log offset exceeds retained prefix: %w", fs.ErrInvalid)
	}
	end := min(offset+int64(limit), int64(len(data)))
	return backend.LogChunk{Data: data[offset:end], NextOffset: end,
		Complete:  (stop != backend.WorkerStopUnproven || truncated) && end == int64(len(data)),
		Truncated: truncated}, nil
}

// workerReadLogs reads the retained prefix of log response resp, at most
// one byte past backend.MaxLogBytes, and closes its body once. A non-2xx
// response is drained to a bound and fails. The error joins the status, read
// and Close failures.
func workerReadLogs(resp *http.Response) ([]byte, error) {
	var data []byte
	var err error
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		_, rerr := io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		err = errors.Join(fmt.Errorf("%w with status %d", errWorkerLogsStatus, resp.StatusCode), rerr)
	} else {
		data, err = io.ReadAll(io.LimitReader(resp.Body, backend.MaxLogBytes+1))
	}
	if cerr := resp.Body.Close(); cerr != nil {
		err = errors.Join(err, fmt.Errorf("close kubernetes worker log stream: %w", cerr))
	}
	if err != nil {
		return nil, err
	}
	return data, nil
}

// openWorkerLogs uses the worker-only client and a fresh single-request gate;
// the caller reads and closes the same original response for every status.
func (rt *workerRuntime) openWorkerLogs(ctx context.Context, name string) (*http.Response, error) {
	client := rt.http.client.CoreV1().RESTClient()
	if limiter := client.GetRateLimiter(); limiter != nil {
		if err := limiter.Wait(ctx); err != nil {
			return nil, err
		}
	}
	u := client.Get().Namespace(rt.b.namespace).Resource("pods").Name(name).SubResource("log").VersionedParams(&corev1.PodLogOptions{Container: "task"}, scheme.ParameterCodec).URL()
	request, err := http.NewRequestWithContext(rt.http.request(ctx), http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	return rt.http.client.CoreV1().RESTClient().(*rest.RESTClient).Client.Do(request)
}
