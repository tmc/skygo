package store

import "strings"

// OriginalServerLifetime identifies the actual server that admitted an owner.
// The zero value means unavailable. Identity alone proves neither process death
// nor descriptor closure; it cannot authorize stopped recovery.
type OriginalServerLifetime struct {
	Host            string
	Boot            string
	NamespaceDevice uint64
	NamespaceInode  uint64
	PID             uint32
	StartTicks      uint64
}

func (p OriginalServerLifetime) valid() bool {
	return artifactHex(p.Host, 32) && p.Host != strings.Repeat("0", 32) &&
		artifactHex(p.Boot, 32) && p.Boot != strings.Repeat("0", 32) &&
		p.NamespaceInode != 0 && p.PID != 0 && p.PID <= 1<<31-1
}

func artifactProducer(p OriginalServerLifetime) (*OriginalServerLifetime, error) {
	if p == (OriginalServerLifetime{}) {
		return nil, nil
	}
	if !p.valid() {
		return nil, ErrArtifactConflict
	}
	return &p, nil
}

func artifactProducerValue(p *OriginalServerLifetime) OriginalServerLifetime {
	if p == nil {
		return OriginalServerLifetime{}
	}
	return *p
}

func artifactProducerValid(p *OriginalServerLifetime) bool { return p == nil || p.valid() }

func artifactCloneProducer(p *OriginalServerLifetime) *OriginalServerLifetime {
	if p == nil {
		return nil
	}
	copy := *p
	return &copy
}
