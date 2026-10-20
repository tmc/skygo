package artifact

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
)

const rootMarkerName = ".skygo-artifact-root"
const markerLimit = 512

// RootFacts binds a fixed installation marker to the actual original directory.
// Unit is its observed filesystem block size and Volume its actual device number.
// MetadataAllowance is a conservative reservation estimate for marker+directory,
// not allocated/reclaimed bytes, a hard capacity bound or a COW guarantee.
type RootFacts struct {
	ID                               string
	File                             FileID
	Unit, Volume                     uint64
	MetadataFiles, MetadataAllowance uint64
}

type rootMarker struct {
	Version uint64 `json:"version"`
	ID      string `json:"id"`
	Device  uint64 `json:"device"`
	Inode   uint64 `json:"inode"`
	Unit    uint64 `json:"unit"`
	Volume  uint64 `json:"volume"`
}

func (f *Files) physicalRoot() (RootFacts, error) {
	if err := f.checkRoot(); err != nil {
		return RootFacts{}, err
	}
	unit, err := filesystemUnit(f.directory)
	if err != nil {
		return RootFacts{}, err
	}
	return RootFacts{File: f.identity, Unit: unit, Volume: f.identity.Device, MetadataFiles: 2, MetadataAllowance: 2 * unit}, nil
}

// RootFacts reads a bounded owned marker and checks actual descriptor facts. The
// caller compares ID/Unit/Volume to Store's installed tuple in a validated snapshot.
// Returning facts does not install a service, grant access or perform adoption.
func (f *Files) RootFacts() (RootFacts, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.ready(); err != nil {
		return RootFacts{}, err
	}
	return f.rootFacts()
}
func (f *Files) rootFacts() (RootFacts, error) {
	return f.rootFactsWithOpen(f.root.OpenFile)
}

// The scanner supplies its original-directory no-follow opener. The canonical
// marker parser, 513-byte read bound and exact positive-close classifier remain
// shared with ordinary installation checks.
func (f *Files) rootFactsWithOpen(open func(string, int, os.FileMode) (*os.File, error)) (RootFacts, error) {
	facts, err := f.physicalRoot()
	if err != nil {
		return RootFacts{}, err
	}
	before, err := f.root.Lstat(rootMarkerName)
	if err != nil {
		return RootFacts{}, err
	}
	check := func(info os.FileInfo) error {
		m, err := fileMetadata(info)
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || info.Mode().Perm() != 0600 || info.Mode()&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != 0 || m.uid != uint32(os.Geteuid()) || m.device != facts.Volume || m.links != 1 || info.Size() < 1 || info.Size() > markerLimit {
			return ErrFileConflict
		}
		return nil
	}
	if err = check(before); err != nil {
		return RootFacts{}, err
	}
	flag, err := nonblockingRead()
	if err != nil {
		return RootFacts{}, err
	}
	file, err := open(rootMarkerName, flag, 0)
	if err != nil {
		return RootFacts{}, err
	}
	info, err := file.Stat()
	if err == nil {
		err = check(info)
	}
	if err == nil && !sameMetadata(before, info) {
		err = ErrFileConflict
	}
	if err == nil {
		err = f.checkPath(rootMarkerName, info)
	}
	if err != nil {
		return RootFacts{}, errors.Join(err, f.closeFile(file))
	}
	data, err := io.ReadAll(io.LimitReader(file, markerLimit+1))
	if err == nil && (len(data) > markerLimit || int64(len(data)) != info.Size()) {
		err = ErrFileConflict
	}
	after, statErr := file.Stat()
	err = errors.Join(err, statErr)
	if statErr == nil && !sameMetadata(info, after) {
		err = errors.Join(err, ErrFileConflict)
	}
	if err == nil {
		err = f.checkPath(rootMarkerName, after)
	}
	err = errors.Join(err, f.closeFile(file))
	if err != nil {
		return RootFacts{}, err
	}
	var marker rootMarker
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&marker); err != nil {
		return RootFacts{}, err
	}
	var extra any
	if err = decoder.Decode(&extra); err != io.EOF {
		return RootFacts{}, ErrFileConflict
	}
	canonical, err := json.Marshal(marker)
	if err != nil {
		return RootFacts{}, err
	}
	canonical = append(canonical, '\n')
	if !bytes.Equal(data, canonical) || marker.Version != 1 || !lowerHex(marker.ID, 32) || marker.Device != facts.File.Device || marker.Inode != facts.File.Inode || marker.Unit != facts.Unit || marker.Volume != facts.Volume {
		return RootFacts{}, ErrFileConflict
	}
	facts.ID = marker.ID
	return facts, nil
}

// Initialize is an explicit stopped, empty-root-only installation operation. It
// The caller must have stopped all prior owners and hold exclusive initialization
// authority; this method cannot prove that handoff. It exclusively creates and
// syncs a marker, never adopts any existing entry. A crash
// or failure retains incomplete state for separately authorized stopped recovery.
// The returned facts are usable only with nil error; Store installation is separate.
func Initialize(root *os.Root, expected FileID, installationID string) (facts RootFacts, err error) {
	if !lowerHex(installationID, 32) {
		return RootFacts{}, ErrFileConflict
	}
	f, err := openFiles(root, expected)
	if err != nil {
		return RootFacts{}, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	entries, readErr := f.directory.ReadDir(1)
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return RootFacts{}, readErr
	}
	if len(entries) != 0 {
		return RootFacts{}, ErrFileConflict
	}
	observed, err := f.physicalRoot()
	if err != nil {
		return RootFacts{}, err
	}
	marker := rootMarker{Version: 1, ID: installationID, Device: observed.File.Device, Inode: observed.File.Inode, Unit: observed.Unit, Volume: observed.Volume}
	data, err := json.Marshal(marker)
	if err != nil {
		return RootFacts{}, err
	}
	data = append(data, '\n')
	if len(data) > markerLimit {
		return RootFacts{}, ErrFileConflict
	}
	file, err := root.OpenFile(rootMarkerName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return RootFacts{}, err
	}
	n, err := file.Write(data)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = file.Sync()
	}
	err = errors.Join(err, f.closeFile(file))
	if err != nil {
		return RootFacts{}, err
	}
	if err = f.syncRoot(); err != nil {
		return RootFacts{}, err
	}
	facts, err = f.rootFacts()
	return
}
