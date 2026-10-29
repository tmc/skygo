package sshagent

import "io"

// A body is a response body a recorder returned. It counts each original
// Read and Close until it returns, keeps the first Read error other than
// io.EOF, runs the original Close at most once and keeps its result:
// Connect discards or replaces those results on some paths. Reading to
// io.EOF closes nothing. Every Close waits for the first and returns its
// result. A failed first Close faults the ledger before Close returns.
//
// A body is read only from within a frame its call entered: Connect reads
// it in Receive, CloseResponse or the unary RPC. A stream's fence does not
// stop those Reads, so frames entered before it can finish.
type body struct {
	c    *call
	rc   io.ReadCloser
	done chan struct{} // closed once err is set

	// guarded by c.l.mu
	readErr error // the first Read error other than io.EOF
	closing bool  // the first Close started
	closed  bool  // the first Close returned
	err     error // its result
}

func (b *body) Read(p []byte) (int, error) {
	l := b.c.l
	l.mu.Lock()
	b.c.enterLocked()
	l.mu.Unlock()
	n, err := b.rc.Read(p)
	l.mu.Lock()
	if err != nil && err != io.EOF && b.readErr == nil {
		b.readErr = err
	}
	b.c.exitLocked()
	l.mu.Unlock()
	return n, err
}

func (b *body) Close() error {
	if b.start() {
		b.run()
	}
	<-b.done
	return b.err
}

// start reports whether the caller is to run the first Close, which no
// one has started yet.
func (b *body) start() bool {
	b.c.l.mu.Lock()
	defer b.c.l.mu.Unlock()
	if b.closing {
		return false
	}
	b.closing = true
	b.c.enterLocked()
	return true
}

// run runs the first Close and records its result.
func (b *body) run() {
	err := b.rc.Close()
	l := b.c.l
	l.mu.Lock()
	b.err, b.closed = err, true
	if err != nil {
		l.faulted = true
	}
	b.c.exitLocked()
	l.mu.Unlock()
	close(b.done)
}
