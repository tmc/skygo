package kube

import (
	"context"
	"errors"

	"github.com/tmc/skygo/internal/backend"
	"k8s.io/client-go/rest"
)

// FromTargetWithState is FromTarget with durable execution receipts for
// ordinary Jobs and retry Pods. stateDir is private server state: an existing
// absolute directory without symlinks, owned by the effective user, with mode
// 0700. Receipts are kept per owner, namespace and target binding.
//
// Before any create, Start durably records the attempt's identity and a
// random creation nonce, then sends exactly one request without client
// retries, without following redirects and with a body that no transport layer
// can obtain again, and records the returned UID. Observe, Cancel, Logs and
// cleanup use only that UID and report it as Status.Instance. A lost create
// response is recovered only from the object carrying the recorded nonce and
// expected specification; until then Observe reports Starting and nothing is
// resent. A complete create response that is not 201 Created with the
// receipted object, a redirection included, is recorded as refused and
// authorizes nothing afterwards.
// The specification is the complete Job or Pod spec; only the API server
// defaults and outputs listed in docs/dra.md may differ.
//
// The target's requests use their own HTTP client, built from the same
// frozen configuration, server, credentials and certificate authority, which
// follows no redirect. Other targets and the default HTTP client are not
// changed.
//
// Every native operation holds a shared permit on the receipt scope until its
// last response, body and receipt write are finished; it waits for cleanup
// only until its context is done. Logs keeps it, for the life of the process,
// if the log response body did not close cleanly. Cleanup takes the permit
// exclusively and fails without waiting while any operation of the scope
// runs, in any Backend or process. Cleanup leaves a retired tombstone that
// keeps its slot and authorizes nothing; a store full of tombstones refuses
// new attempts. Every acquisition rechecks that the state and scope
// directories are the private directories found at construction.
//
// The mode never adopts by name, labels or task digest. Objects created
// without receipts, including those of FromTarget, are refused, as are
// attempts whose receipt is missing, corrupt, foreign or already present.
// Targets with a data block are refused. Receipts require linux or darwin.
func FromTargetWithState(ctx context.Context, t TargetConfig, owner, stateDir string) (backend.Target, error) {
	if err := checkReceiptRoot(stateDir); err != nil {
		return backend.Target{}, err
	}
	if t.Data != nil {
		return backend.Target{}, errors.New("kubernetes execution receipts do not cover data targets")
	}
	target, config, err := loadTarget(ctx, t, owner)
	if err != nil {
		return backend.Target{}, err
	}
	return withReceipts(target, config, stateDir)
}

// withReceipts attaches a receipt store and the receipt client built from
// config, the frozen configuration target was built from, to the actual
// Backend of target, so that its Retention and other capabilities keep
// referring to it.
func withReceipts(target backend.Target, config *rest.Config, stateDir string) (backend.Target, error) {
	b, ok := target.Backend.(*Backend)
	if !ok || b == nil || b.data != nil || config == nil {
		return backend.Target{}, errors.New("kubernetes execution receipts require a plain kubernetes target")
	}
	client, err := newReceiptClient(config)
	if err != nil {
		return backend.Target{}, errors.New("create kubernetes client")
	}
	s, err := openReceiptStore(stateDir, b.owner, b.namespace, b.binding)
	if err != nil {
		return backend.Target{}, err
	}
	b.client, b.receipts = client, s
	return target, nil
}
