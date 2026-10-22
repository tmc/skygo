package auth

// Delegation identifies an exact file credential for trusted server persistence.
// It contains no plaintext token and grants no ordinary Actor authority. Its zero
// value is invalid. A current Engine must check it before each delegated action.
type Delegation struct {
	Principal, TokenID string
	Digest             [32]byte
}

// Delegate captures an exact file credential after checking fresh Mutate
// authority on this Engine. Anonymous and legacy engines cannot delegate.
func (e *Engine) Delegate(actor Actor) (Delegation, error) {
	checked, err := e.Check(actor, Mutate)
	if err != nil {
		return Delegation{}, err
	}
	if e.legacy || checked.tokenID == "" {
		return Delegation{}, ErrUnauthenticated
	}
	return Delegation{Principal: checked.principal, TokenID: checked.tokenID, Digest: checked.digest}, nil
}

// CheckDelegation checks the current fresh file snapshot, exact credential and
// current Operator/Admin role. It returns no Actor and cannot authenticate a
// user-supplied delegation. The same persisted value may be checked after restart
// against the same installation's actual credential file.
func (e *Engine) CheckDelegation(d Delegation) error {
	if e == nil || d.Principal == "" || d.TokenID == "" {
		return ErrUnauthenticated
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.legacy {
		return ErrUnauthenticated
	}
	if !e.readyLocked() {
		return ErrUnavailable
	}
	for _, token := range e.snapshot.Tokens {
		if token.ID != d.TokenID || token.Principal != d.Principal || !equalDigest(token.digest, d.Digest) || !token.usable(e.now()) {
			continue
		}
		role := e.snapshot.role(token.Principal)
		if role != Operator && role != Admin {
			return ErrPermissionDenied
		}
		return nil
	}
	return ErrUnauthenticated
}
