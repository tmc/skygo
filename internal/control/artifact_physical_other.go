//go:build !linux && !darwin

package control

import "time"

func (c *Control) samplePhysicalDatabase() {
	p := &c.artifacts.physical
	p.mu.Lock()
	defer p.mu.Unlock()
	p.points.databaseBytes = nil
	p.points.databaseFree = nil
	p.points.databaseAt = time.Time{}
	p.points.databaseFreeAt = time.Time{}
}
