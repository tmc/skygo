package artifact

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"time"
)

const (
	physicalStepVisits  = 4096
	physicalPassVisits  = 65536
	physicalDepth       = 64
	physicalFullQuanta  = 16
	physicalTotalQuanta = 32
	physicalPayload     = 16 << 20
	physicalDuration    = 2 * time.Minute
	physicalRecordBytes = 160
	physicalRootEntries = 4096
	physicalEntryBytes  = 64
)

// PhysicalStatus describes interval observations, never a current atomic total.
type PhysicalStatus uint8

const (
	PhysicalUnavailable PhysicalStatus = iota
	PhysicalPartial
	PhysicalIntervalComplete
)

// PhysicalEntryType is the no-follow type of an observed first-level entry.
// Other includes symlinks, special files and unrecognized types.
type PhysicalEntryType uint8

const (
	PhysicalEntryOther PhysicalEntryType = iota
	PhysicalEntryRegular
	PhysicalEntryDirectory
)

// PhysicalEntry is an observation, not authority to open, adopt or remove a name.
// Name is a first-level basename; File is its observed no-follow identity.
type PhysicalEntry struct {
	Name string
	File FileID
	Type PhysicalEntryType
}

// PhysicalSample reports allocated inode blocks observed over a bounded interval.
// Nil numeric fields mean unavailable, not zero. A partial count is diagnostic
// only. Names counts entries; Inodes deduplicates actual device/inode pairs.
// Times belong only to their actual observations. FreeObservedAt never renews an
// older inventory. Concurrent rename/write can evade an interval walk; this is
// neither exclusive storage usage, peak usage, a quota nor a cleanup receipt.
type PhysicalSample struct {
	Root                               RootFacts
	Status                             PhysicalStatus
	StartedAt, EndedAt                 time.Time
	OldestObservedAt, NewestObservedAt time.Time
	AllocatedBytes, Inodes             *uint64
	Visits, Names                      uint64
	FreeBytes                          *uint64
	FreeObservedAt                     time.Time
	Conditions                         []string
	// RootEntries includes every consumed first-level name on complete intervals,
	// including the marker. Partial projections are diagnostic only. The returned
	// slice is independent of this scan and every other returned sample. A complete
	// interval is not simultaneous or all-live-name coverage under concurrent rename.
	RootEntries []PhysicalEntry
}

// PhysicalScan owns one bounded, descriptor-anchored original directory cursor.
// Its zero value is unusable. Obtain it from BeginPhysicalSample. Step/Close
// serialize actual I/O; no timeout abandons an operation. A yielded or completed
// cursor still blocks Files.Close until its own Close positively returns nil.
// Every operation requires the caller's actual common artifact I/O permit.
// ErrFileCloseUncertain retains that same active permit, original scan and error;
// no replacement scanner or raw descriptor retry is permitted.
type PhysicalScan struct {
	files                *Files
	frames               []physicalFrame
	seen                 map[FileID]physicalRecord
	root                 RootFacts
	marker               FileID
	started, last        time.Time
	oldest, newest       time.Time
	bytes, visits, names uint64
	payload              int
	quanta, full         int
	done, closed         bool
	err, closeErr        error
	conditions           []string
	rootEntries          []PhysicalEntry
}

type physicalFrame struct {
	file    *os.File
	name    string
	id      FileID
	entries []string
	eof     bool
}

type physicalRecord struct {
	fact        physicalObservation
	first, last time.Time
}

type physicalObservation struct {
	id                                                 FileID
	blocks                                             uint64
	size                                               int64
	mode                                               uint32
	uid                                                uint32
	links                                              uint64
	modifiedSec, modifiedNsec, changedSec, changedNsec int64
}

// Fault seams are package-private, installed only before sharing a test fixture.
// The production path always calls the concrete descriptor operations below.
type physicalIO struct {
	now        func() time.Time
	readDir    func(*os.File, int) ([]string, error)
	statAt     func(*os.File, string) (physicalObservation, error)
	openDir    func(*os.File, string) (*os.File, error)
	openMarker func(*os.File, string) (*os.File, error)
	free       func(*os.File) (uint64, error)
}

func (s *PhysicalScan) now() time.Time {
	if s.files.physicalIO.now != nil {
		return s.files.physicalIO.now()
	}
	return time.Now()
}

// BeginPhysicalSample registers the only scan before opening a scan-owned FD.
// It uses a distinct directory stream, never Files.directory's ReadDir offset.
// On any error after registration it returns the same non-nil scan: close it
// positively, or retain it and the original permit on uncertain Close. It never
// adopts a Local execution owner or proves that observed names match Store's
// effective cohort; the caller must separately establish that binding.
func (f *Files) BeginPhysicalSample() (*PhysicalScan, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.ready(); err != nil {
		return nil, err
	}
	if f.physical != nil {
		return nil, ErrFilesBusy
	}
	s := &PhysicalScan{files: f, seen: make(map[FileID]physicalRecord)}
	f.physical = s
	s.started = s.now()
	s.last = s.started
	facts, err := s.rootFacts()
	if err != nil {
		s.fail("root", err)
		return s, err
	}
	s.root = facts
	file, err := s.openDir(f.directory, ".")
	if err != nil {
		s.fail("open", err)
		return s, err
	}
	// Register before Stat or any failing check can lose this original FD.
	s.frames = append(s.frames, physicalFrame{file: file, id: facts.File})
	s.payload += 128
	observed, err := physicalDescriptor(file)
	if err == nil && (observed.id != facts.File || !physicalDirectory(observed)) {
		err = ErrFileConflict
	}
	if err == nil {
		err = s.observe(observed, s.now())
	}
	if err != nil {
		s.fail("root", err)
		return s, err
	}
	if err = s.deadline(); err != nil {
		s.fail("deadline", err)
		return s, err
	}
	return s, nil
}

func (s *PhysicalScan) openDir(parent *os.File, name string) (*os.File, error) {
	if s.files.physicalIO.openDir != nil {
		return s.files.physicalIO.openDir(parent, name)
	}
	return openPhysicalDirectory(parent, name)
}
func (s *PhysicalScan) statAt(parent *os.File, name string) (physicalObservation, error) {
	if s.files.physicalIO.statAt != nil {
		return s.files.physicalIO.statAt(parent, name)
	}
	return physicalAt(parent, name)
}
func (s *PhysicalScan) rootFacts() (RootFacts, error) {
	f := s.files
	if f.hasUncertainClose() {
		return RootFacts{}, ErrFilesBusy
	}
	var marker FileID
	facts, err := f.rootFactsWithOpen(func(name string, _ int, _ os.FileMode) (*os.File, error) {
		// Marker and child opens never overlap. Only the bounded marker is read.
		var file *os.File
		var err error
		if f.physicalIO.openMarker != nil {
			file, err = f.physicalIO.openMarker(f.directory, name)
		} else {
			file, err = openPhysicalMarker(f.directory, name)
		}
		if err != nil {
			return nil, err
		}
		observed, err := physicalDescriptor(file)
		if err == nil {
			marker = observed.id
			if s.marker.valid() && marker != s.marker {
				err = ErrFileConflict
			}
		}
		if err != nil {
			return nil, errors.Join(err, f.closeFile(file))
		}
		return file, nil
	})
	if err == nil {
		s.marker = marker
	}
	return facts, err
}
func (s *PhysicalScan) deadline() error {
	now := s.now()
	// Equal times are allowed; UTC rollback or elapsed monotonic ceiling is not.
	if now.UTC().Before(s.last.UTC()) || now.Sub(s.started) < 0 || now.Sub(s.started) > physicalDuration || now.UTC().Sub(s.started.UTC()) > physicalDuration {
		return errors.New("artifact physical interval expired")
	}
	s.last = now
	return nil
}
func (s *PhysicalScan) condition(name string) {
	for _, c := range s.conditions {
		if c == name {
			return
		}
	}
	if len(s.conditions) < 8 {
		s.conditions = append(s.conditions, name)
	}
}
func (s *PhysicalScan) fail(condition string, err error) {
	s.done = true
	s.condition(condition)
	s.err = errors.Join(s.err, err)
}
func (s *PhysicalScan) observe(o physicalObservation, now time.Time) error {
	if !o.id.valid() || o.id.Device != s.root.Volume || o.uid != uint32(os.Geteuid()) {
		return ErrFileConflict
	}
	if old, ok := s.seen[o.id]; ok {
		if old.fact != o {
			return errors.New("artifact physical inode changed between observations")
		}
		old.last = now
		s.seen[o.id] = old
		s.newest = now
		return nil
	}
	if len(s.seen) >= physicalPassVisits || s.payload > physicalPayload-physicalRecordBytes {
		return errors.New("artifact physical record limit")
	}
	if o.blocks > math.MaxUint64/512 || s.bytes > math.MaxUint64-o.blocks*512 {
		return errors.New("artifact physical block overflow")
	}
	s.seen[o.id] = physicalRecord{fact: o, first: now, last: now}
	s.payload += physicalRecordBytes
	s.bytes += o.blocks * 512
	if s.oldest.IsZero() {
		s.oldest = now
	}
	s.newest = now
	return nil
}
func (s *PhysicalScan) observeRootEntry(name string, o physicalObservation) error {
	if len(s.rootEntries) >= physicalRootEntries {
		return errors.New("artifact physical root-entry limit")
	}
	if s.payload > physicalPayload-len(name)-physicalEntryBytes {
		return errors.New("artifact physical root-entry payload limit")
	}
	s.rootEntries = append(s.rootEntries, PhysicalEntry{Name: name, File: o.id, Type: physicalEntryType(o)})
	s.payload += len(name) + physicalEntryBytes
	return nil
}
func (s *PhysicalScan) checkFrame(index int) error {
	for i := 0; i <= index; i++ {
		if err := s.checkOneFrame(i); err != nil {
			return err
		}
	}
	return nil
}
func (s *PhysicalScan) checkOneFrame(index int) error {
	frame := &s.frames[index]
	o, err := physicalDescriptor(frame.file)
	if err != nil {
		return err
	}
	if o.id != frame.id || !physicalDirectory(o) || o.id.Device != s.root.Volume || o.uid != uint32(os.Geteuid()) {
		return ErrFileConflict
	}
	if index == 0 {
		return s.files.checkRoot()
	}
	path, err := s.statAt(s.frames[index-1].file, frame.name)
	if err != nil {
		return err
	}
	if path.id != frame.id || !physicalDirectory(path) {
		return ErrFileConflict
	}
	return nil
}
func (s *PhysicalScan) point() (free *uint64, at time.Time, err error) {
	facts, err := s.rootFacts()
	if err != nil {
		return nil, time.Time{}, err
	}
	if facts != s.root {
		return nil, time.Time{}, ErrFileConflict
	}
	var n uint64
	if s.files.physicalIO.free != nil {
		n, err = s.files.physicalIO.free(s.files.directory)
	} else {
		n, err = physicalFree(s.files.directory)
	}
	if err != nil {
		return nil, time.Time{}, err
	}
	if err = s.files.checkRoot(); err != nil {
		return nil, time.Time{}, err
	}
	return &n, s.now(), nil
}

// Step visits at most 4096 real names, in directory-name batches of at most 64.
// Readdirnames avoids ReadDir's host-path fallback on unknown d_type filesystems;
// all inode observations use descriptor-relative no-follow Fstatat instead. It keeps
// the original stack/offset across yields. At most 65 directory FDs and one
// temporary child or marker FD are owned at a time. It opens no workload file,
// follows no symlink and performs no workload content read or archive parsing.
// Successful exhaustion is an interval observation, not a simultaneous census.
// A failed/over-limit interval cannot refresh its complete timestamp. Close is
// still required after every terminal status/error, including successful EOF.
func (s *PhysicalScan) Step(ctx context.Context) (PhysicalSample, error) {
	if s == nil || s.files == nil {
		return PhysicalSample{}, ErrFilesClosed
	}
	f := s.files
	f.mu.Lock()
	defer f.mu.Unlock()
	if s.closed || f.physical != s {
		return PhysicalSample{}, ErrFilesClosed
	}
	if s.done {
		return s.sample(nil, time.Time{}), s.err
	}
	if f.closing || f.closed {
		s.fail("owner", ErrFilesClosed)
		return s.sample(nil, time.Time{}), s.err
	}
	s.quanta++
	if s.quanta > physicalTotalQuanta {
		s.fail("quanta", errors.New("artifact physical quantum limit"))
	}
	// Point failures invalidate inventory but do not stand in for a DB point;
	// Control owns that independent measurement outside this Files operation.
	if _, _, err := s.point(); err != nil {
		s.fail("root", err)
	}
	if err := f.ready(); err != nil {
		s.fail("owner", err)
	}
	if err := s.deadline(); err != nil {
		s.fail("deadline", err)
	}
	used := 0
	for !s.done && len(s.frames) > 0 && used < physicalStepVisits {
		if err := ctx.Err(); err != nil {
			s.fail("context", err)
			break
		}
		if err := s.deadline(); err != nil {
			s.fail("deadline", err)
			break
		}
		i := len(s.frames) - 1
		if err := s.checkFrame(i); err != nil {
			s.fail("identity", err)
			break
		}
		frame := &s.frames[i]
		if len(frame.entries) == 0 && !frame.eof {
			var entries []string
			var err error
			if f.physicalIO.readDir != nil {
				entries, err = f.physicalIO.readDir(frame.file, 64)
			} else {
				entries, err = frame.file.Readdirnames(64)
			}
			if len(entries) > 64 {
				s.fail("batch", ErrFileConflict)
				break
			}
			if err != nil && !errors.Is(err, io.EOF) {
				s.fail("read", err)
				break
			}
			for _, entry := range entries {
				if s.payload > physicalPayload-len(entry)-64 {
					s.fail("payload", errors.New("artifact physical entry payload limit"))
					break
				}
				s.payload += len(entry) + 64
			}
			if s.done {
				break
			}
			frame.entries = entries
			frame.eof = errors.Is(err, io.EOF)
			if len(entries) == 0 && err == nil {
				s.fail("read", io.ErrNoProgress)
				break
			}
		}
		// Readdirnames can positively return after a directory moved. Recheck
		// its original ancestry before consuming names or accepting EOF.
		if err := s.checkFrame(i); err != nil {
			s.fail("identity", err)
			break
		}
		if len(frame.entries) == 0 {
			// Do not drop the last frame until final marker validation. Closing
			// the root here would leave a zero-FD cursor before final checks.
			if i == 0 {
				s.done = true
				break
			}
			if err := f.closeFile(frame.file); err != nil {
				s.fail("close", err)
				break
			}
			s.payload -= len(frame.name) + 128
			s.frames = s.frames[:i]
			continue
		}
		if s.visits >= physicalPassVisits {
			s.fail("visits", errors.New("artifact physical visit limit"))
			break
		}
		entry := frame.entries[0]
		frame.entries[0] = ""
		frame.entries = frame.entries[1:]
		name := entry
		s.payload -= len(name) + 64
		used++
		s.visits++
		s.names++
		if name == "" || name == "." || name == ".." || strings.ContainsAny(name, "/\x00") || len(name) > 255 {
			s.fail("name", ErrFileConflict)
			break
		}
		o, err := s.statAt(frame.file, name)
		if err != nil {
			s.fail("stat", err)
			break
		}
		if i == 0 && name == rootMarkerName && o.id != s.marker {
			s.fail("marker", ErrFileConflict)
			break
		}
		if err = s.observe(o, s.now()); err != nil {
			s.fail("inode", err)
			break
		}
		if i == 0 {
			if err = s.observeRootEntry(name, o); err != nil {
				s.fail("root-entries", err)
				break
			}
		}
		if !physicalDirectory(o) {
			continue
		} // symlinks/specials counted, never opened
		if i >= physicalDepth {
			s.fail("depth", errors.New("artifact physical depth limit"))
			break
		}
		if s.payload > physicalPayload-len(name)-128 {
			s.fail("payload", errors.New("artifact physical payload limit"))
			break
		}
		child, err := s.openDir(frame.file, name)
		if err != nil {
			s.fail("open", err)
			break
		}
		// Only this temporary FD exists; put it under scan ownership before
		// descriptor/path checks, including any failing Close.
		s.frames = append(s.frames, physicalFrame{file: child, id: o.id, name: name})
		s.payload += len(name) + 128
		after, err := physicalDescriptor(child)
		if err == nil && (after.id != o.id || !physicalDirectory(after) || after.id.Device != s.root.Volume) {
			err = ErrFileConflict
		}
		if err == nil {
			err = s.checkFrame(len(s.frames) - 1)
		}
		if err != nil {
			s.fail("identity", err)
			break
		}
	}
	if used == physicalStepVisits {
		s.full++
		if s.full > physicalFullQuanta {
			s.fail("quanta", errors.New("artifact physical full-quantum limit"))
		}
	}
	// Fresh actual point after positively returning Step, even after inventory
	// limits; an earlier successful point never receives this completion time.
	free, at, err := s.point()
	if err != nil {
		s.fail("root", err)
	}
	if e := s.deadline(); e != nil {
		s.fail("deadline", e)
	}
	if s.done && s.err == nil {
		// Complete numeric status requires positive original descriptor close;
		// registration remains until explicit Close, independently of numbers.
		for i := len(s.frames) - 1; i >= 0; i-- {
			if e := f.closeFile(s.frames[i].file); e != nil {
				s.fail("close", e)
				break
			}
			s.frames = s.frames[:i]
		}
		if e := s.deadline(); e != nil {
			s.fail("deadline", e)
		}
	}
	if errors.Is(s.err, ErrFileCloseUncertain) {
		free = nil
		at = time.Time{}
	}
	return s.sample(free, at), s.err
}
func (s *PhysicalScan) sample(free *uint64, at time.Time) PhysicalSample {
	status := PhysicalPartial
	if len(s.seen) == 0 {
		status = PhysicalUnavailable
	}
	var ended time.Time
	if s.done && s.err == nil {
		status = PhysicalIntervalComplete
		ended = s.last.UTC()
	}
	b, n := s.bytes, uint64(len(s.seen))
	var allocated, inodes *uint64
	if len(s.seen) > 0 {
		allocated, inodes = &b, &n
	}
	return PhysicalSample{Root: s.root, Status: status, StartedAt: s.started.UTC(), EndedAt: ended, OldestObservedAt: s.oldest.UTC(), NewestObservedAt: s.newest.UTC(), AllocatedBytes: allocated, Inodes: inodes, Visits: s.visits, Names: s.names, FreeBytes: free, FreeObservedAt: at.UTC(), Conditions: append([]string(nil), s.conditions...), RootEntries: append([]PhysicalEntry(nil), s.rootEntries...)}
}

// Close joins only this scan's original descriptors. It never closes the borrowed
// Root/Files.directory and never retries an uncertain original numeric FD.
// Registration clears only after all original closes positively succeed. A
// sticky FileCloseError keeps this scan, Files root obligation and common active
// I/O permit alive for separately authorized stopped handoff.
func (s *PhysicalScan) Close() error {
	if s == nil || s.files == nil {
		return ErrFilesClosed
	}
	f := s.files
	f.mu.Lock()
	defer f.mu.Unlock()
	if s.closed || s.closeErr != nil {
		return s.closeErr
	}
	for i := len(s.frames) - 1; i >= 0; i-- {
		if s.frames[i].file == nil {
			continue
		}
		if err := f.closeFile(s.frames[i].file); err != nil {
			s.closeErr = errors.Join(s.closeErr, err)
			continue
		}
		s.frames[i].file = nil
	}
	// A previous marker/temporary Close may be retained by the Files registry.
	if errors.Is(s.err, ErrFileCloseUncertain) {
		s.closeErr = errors.Join(s.closeErr, s.err)
	}
	if s.closeErr != nil {
		return s.closeErr
	}
	s.frames = nil
	s.closed = true
	if f.physical != s {
		return fmt.Errorf("close artifact physical scan: %w", ErrFileConflict)
	}
	f.physical = nil
	return nil
}
