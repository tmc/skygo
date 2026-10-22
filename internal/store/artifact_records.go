package store

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"math"
	"time"

	bolt "go.etcd.io/bbolt"
)

var artifactsBucket = []byte("artifacts")

// artifactFormat is the current artifact root floor. It is separate from the
// database schema: schema9 added only environment metadata and kept floor8.
const artifactFormat = 8

// These counters are independent of job retention and remain mandatory when
// retention is disabled. Fixed-width counters do not change their own charge.
const (
	aPendingBytes = iota
	aPendingCount
	aCompletedBytes
	aCompletedCount
	aMetadataBytes
	aMetadataRecords
	aDownloads
	aWorkspaces
	aAllocatedBytes
	aFiles
	aUsageCount
)

type artifactUsage [aUsageCount]uint64

func artifactRootUsage(root artifactRoot) artifactUsage {
	var u artifactUsage
	u[aMetadataBytes] = 2048 + 5 + 80 + 5
	u[aMetadataRecords] = 2
	// Conservative marker+root-directory allowance from Files.RootFacts,
	// never an assertion of observed allocation or free space.
	u[aAllocatedBytes] = 2 * root.Unit
	u[aFiles] = 2
	return u
}

type artifactState struct {
	Root         artifactRoot
	Uploads      map[string]artifactUploadRecord
	Objects      map[string]artifactObject
	Grants       map[string]artifactGrant
	Reads        map[string]artifactRead
	JobInputs    map[string]artifactPin
	ManualInputs map[string]artifactPin
	Results      map[string]artifactResult
	Expired      map[string]artifactExpiredResult
	Workspaces   map[string]artifactWorkspace
	Completions  map[string]artifactCompletion
	Remote       map[string]remoteRecord
	Deletes      map[string]artifactDelete
	// WorkflowInputs are input edges prepaid at graph acceptance.
	WorkflowInputs map[string]workflowInput
}

type artifactRows map[string]map[string][]byte

var artifactTables = []struct {
	name string
	max  uint64
}{
	{"uploads", 2048}, {"objects", 1536}, {"grants", 1024}, {"reads", 1024},
	{"job_inputs", 1024}, {"manual_inputs", 1024}, {"results", 1536},
	{"result_expired", 512}, {"workspaces", 4096}, {"completions", 1024}, {"gc", 2048}, {"owners", 80}, {"remote_attempts", 4096},
	{"workflow_inputs", 1024},
}

func artifactCorrupt() error { return errors.New("invalid artifact metadata") }

func artifactDecode(raw []byte, out any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(out); err != nil {
		return artifactCorrupt()
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return artifactCorrupt()
	}
	return nil
}

func artifactReadMap[T any](b *bolt.Bucket) (map[string]T, error) {
	out := make(map[string]T)
	if b == nil {
		return nil, artifactCorrupt()
	}
	err := b.ForEach(func(k, v []byte) error {
		if v == nil || !artifactHex(string(k), 32) {
			return artifactCorrupt()
		}
		var r T
		if err := artifactDecode(v, &r); err != nil {
			return err
		}
		out[string(k)] = r
		return nil
	})
	return out, err
}

func artifactLoad(tx *bolt.Tx) (*artifactState, error) {
	b := tx.Bucket(artifactsBucket)
	if schemaNumber(tx) > schemaVersion {
		return nil, ErrUnsupportedSchema
	}
	if schemaNumber(tx) <= 5 || b == nil {
		return nil, ErrArtifactsDisabled
	}
	// The genuine legacy layout must be checked before migration adds a table.
	tables := artifactTables
	floor := schemaNumber(tx)
	switch floor {
	case 6:
		tables = tables[:len(tables)-2]
	case 7:
		tables = tables[:len(tables)-1]
	case artifactFormat, schemaVersion:
		floor = artifactFormat
	default:
		return nil, ErrUnsupportedSchema
	}
	s := new(artifactState)
	if err := artifactDecode(b.Get([]byte("state")), &s.Root); err != nil {
		return nil, err
	}
	if s.Root.Layout != 1 || s.Root.Policy != "default-v1" || s.Root.SchemaFloor != floor || !artifactHex(s.Root.ID, 32) || s.Root.Unit < 512 || s.Root.Unit > 65536 || s.Root.Unit&(s.Root.Unit-1) != 0 || s.Root.Volume == 0 {
		return nil, artifactCorrupt()
	}
	known := map[string]bool{"state": true, "usage": true}
	for _, t := range tables {
		known[t.name] = true
	}
	if err := b.ForEach(func(k, v []byte) error {
		if !known[string(k)] || (string(k) == "state" || string(k) == "usage") != (v != nil) {
			return artifactCorrupt()
		}
		return nil
	}); err != nil {
		return nil, err
	}
	var count, rawBytes uint64
	for _, t := range tables {
		table := b.Bucket([]byte(t.name))
		if table == nil {
			return nil, artifactCorrupt()
		}
		if err := table.ForEach(func(k, v []byte) error {
			if v == nil || uint64(len(v)) > t.max {
				return artifactCorrupt()
			}
			count++
			rawBytes += uint64(len(k) + len(v))
			if count > 4096 || rawBytes > 2<<20 {
				return artifactCorrupt()
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}
	var err error
	if s.Uploads, err = artifactReadMap[artifactUploadRecord](b.Bucket([]byte("uploads"))); err != nil {
		return nil, err
	}
	if s.Objects, err = artifactReadMap[artifactObject](b.Bucket([]byte("objects"))); err != nil {
		return nil, err
	}
	if s.Grants, err = artifactReadMap[artifactGrant](b.Bucket([]byte("grants"))); err != nil {
		return nil, err
	}
	if s.Reads, err = artifactReadMap[artifactRead](b.Bucket([]byte("reads"))); err != nil {
		return nil, err
	}
	if s.JobInputs, err = artifactReadMap[artifactPin](b.Bucket([]byte("job_inputs"))); err != nil {
		return nil, err
	}
	if s.ManualInputs, err = artifactReadMap[artifactPin](b.Bucket([]byte("manual_inputs"))); err != nil {
		return nil, err
	}
	if s.Results, err = artifactReadMap[artifactResult](b.Bucket([]byte("results"))); err != nil {
		return nil, err
	}
	if s.Expired, err = artifactReadMap[artifactExpiredResult](b.Bucket([]byte("result_expired"))); err != nil {
		return nil, err
	}
	if s.Workspaces, err = artifactReadMap[artifactWorkspace](b.Bucket([]byte("workspaces"))); err != nil {
		return nil, err
	}
	if s.Completions, err = artifactReadMap[artifactCompletion](b.Bucket([]byte("completions"))); err != nil {
		return nil, err
	}
	if s.Deletes, err = artifactReadMap[artifactDelete](b.Bucket([]byte("gc"))); err != nil {
		return nil, err
	}
	if schemaNumber(tx) >= 7 {
		if s.Remote, err = artifactReadMap[remoteRecord](b.Bucket([]byte("remote_attempts"))); err != nil {
			return nil, err
		}
	} else {
		s.Remote = make(map[string]remoteRecord)
		for _, r := range s.Uploads {
			if r.RemoteAttemptID != "" {
				return nil, artifactCorrupt()
			}
		}
		for _, r := range s.Reads {
			if r.RemoteAttemptID != "" {
				return nil, artifactCorrupt()
			}
		}
		for _, r := range s.Objects {
			if r.Origin == "remote_result" {
				return nil, artifactCorrupt()
			}
		}
	}
	if schemaNumber(tx) >= 8 {
		if s.WorkflowInputs, err = artifactReadMap[workflowInput](b.Bucket([]byte("workflow_inputs"))); err != nil {
			return nil, err
		}
	} else {
		s.WorkflowInputs = make(map[string]workflowInput)
	}
	rows, usage, owners, err := s.encode()
	if err != nil {
		return nil, err
	}
	// The sole writer uses canonical JSON. Refuse alternate/duplicate-key
	// encodings so the changed-value ledger uses exact stored old bytes.
	rootBytes, e := json.Marshal(s.Root)
	if e != nil || len(rootBytes) > 2048 || !bytes.Equal(rootBytes, b.Get([]byte("state"))) {
		return nil, artifactCorrupt()
	}
	for _, t := range tables {
		if t.name == "owners" {
			continue
		}
		if err := b.Bucket([]byte(t.name)).ForEach(func(k, v []byte) error {
			if !bytes.Equal(v, rows[t.name][string(k)]) {
				return artifactCorrupt()
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}
	if !bytes.Equal(b.Get([]byte("usage")), artifactUsageBytes(usage)) {
		return nil, artifactCorrupt()
	}
	ob := b.Bucket([]byte("owners"))
	if ob == nil {
		return nil, artifactCorrupt()
	}
	n := 0
	if err := ob.ForEach(func(k, v []byte) error {
		u, ok := owners[string(k)]
		if !ok || !bytes.Equal(v, artifactUsageBytes(u)) {
			return artifactCorrupt()
		}
		n++
		return nil
	}); err != nil {
		return nil, err
	}
	if n != len(owners) {
		return nil, artifactCorrupt()
	}
	if err := artifactValidateCore(tx, s); err != nil {
		return nil, err
	}
	if schemaNumber(tx) >= 7 {
		if err := remoteValidateCore(tx, s, nil); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func artifactAdd(dst *uint64, n uint64) error {
	if n > math.MaxUint64-*dst {
		return artifactCorrupt()
	}
	*dst += n
	return nil
}

func artifactAllocation(n, unit uint64) (uint64, error) {
	if unit == 0 || n > artifactMaxSize {
		return 0, artifactCorrupt()
	}
	return ((n+unit-1)/unit + 2) * unit, nil
}

func artifactUsageBytes(u artifactUsage) []byte {
	b := make([]byte, aUsageCount*8)
	for i, n := range u {
		binary.BigEndian.PutUint64(b[i*8:], n)
	}
	return b
}

func (s *artifactState) encode() (artifactRows, artifactUsage, map[string]artifactUsage, error) {
	rows := make(artifactRows)
	owners := make(map[string]artifactUsage)
	usage := artifactRootUsage(s.Root)
	put := func(table, key, owner string, value any) error {
		if !artifactHex(key, 32) || !validPrincipal([]byte(owner)) {
			return artifactCorrupt()
		}
		var max uint64
		for _, t := range artifactTables {
			if t.name == table {
				max = t.max
			}
		}
		b, err := json.Marshal(value)
		if table == "remote_attempts" {
			r, ok := value.(remoteRecord)
			if !ok {
				return artifactCorrupt()
			}
			b, err = remoteEncode(r)
		}
		if err != nil || uint64(len(b)) > max || max == 0 {
			return artifactCorrupt()
		}
		if rows[table] == nil {
			rows[table] = make(map[string][]byte)
		}
		rows[table][key] = b
		u := owners[owner]
		if err := artifactAdd(&u[aMetadataBytes], max+uint64(len(key))); err != nil {
			return err
		}
		if err := artifactAdd(&u[aMetadataRecords], 1); err != nil {
			return err
		}
		owners[owner] = u
		return nil
	}
	charge := func(owner string, domain int, n uint64) error {
		u := owners[owner]
		if err := artifactAdd(&u[domain], n); err != nil {
			return err
		}
		owners[owner] = u
		return nil
	}
	allocation := func(owner string, size uint64) error {
		n, err := artifactAllocation(size, s.Root.Unit)
		if err != nil {
			return err
		}
		if err := charge(owner, aAllocatedBytes, n); err != nil {
			return err
		}
		return charge(owner, aFiles, 1)
	}
	if len(s.Remote) > 1024 {
		return nil, usage, nil, artifactCorrupt()
	}
	for id, r := range s.Remote {
		owner, err := remoteRecordOwner(s, id, r)
		if err != nil {
			return nil, usage, nil, err
		}
		if _, exists := s.Workspaces[id]; exists {
			return nil, usage, nil, artifactCorrupt()
		}
		if _, exists := s.Completions[id]; exists {
			return nil, usage, nil, artifactCorrupt()
		}
		if err := put("remote_attempts", id, owner, r); err != nil {
			return nil, usage, nil, err
		}
		// Accepted and closed-but-not-retired attempts retain their slot. Remote
		// Pods do not claim local inode, allocation, or filesystem evidence.
		if r.State != 4 {
			if err := charge(owner, aWorkspaces, 1); err != nil {
				return nil, usage, nil, err
			}
		}
	}
	for id, r := range s.Uploads {
		if r.Ref.ID != id || !artifactRefValid(r.Ref) || r.Revision == 0 || r.Created.IsZero() || r.RemoteAttemptID == "" && !r.Expires.Equal(r.Created.Add(time.Hour)) || r.Ack > r.Ref.Size {
			return nil, usage, nil, artifactCorrupt()
		}
		switch r.State {
		case "creating":
			if r.Ack != 0 || r.Part != (ArtifactFile{}) {
				return nil, usage, nil, artifactCorrupt()
			}
		case "writable", "finalizing":
			if !artifactFileValid(r.Part) {
				return nil, usage, nil, artifactCorrupt()
			}
		case "deleting":
			if r.Part != (ArtifactFile{}) && !artifactFileValid(r.Part) {
				return nil, usage, nil, artifactCorrupt()
			}
		case "complete":
			if o, ok := s.Objects[id]; !ok || o.Ref != r.Ref || o.Owner != r.Owner || (o.Origin != "upload" && o.Origin != "remote_result") || o.State != "complete" && o.State != "deleting" {
				return nil, usage, nil, artifactCorrupt()
			}
		default:
			return nil, usage, nil, artifactCorrupt()
		}
		if r.RemoteAttemptID != "" {
			if !artifactHex(r.RemoteAttemptID, 32) || !remoteCheckTime(r.Created) || !remoteCheckTime(r.Expires) || !r.Expires.After(r.Created) || r.Expires.Sub(r.Created) > 600*time.Second {
				return nil, usage, nil, artifactCorrupt()
			}
			rr, present := s.Remote[r.RemoteAttemptID]
			o, exists := s.Objects[id]
			if present {
				owner, err := remoteRecordOwner(s, r.RemoteAttemptID, rr)
				if err != nil || owner != r.Owner || remoteOutputID(rr) != id {
					return nil, usage, nil, artifactCorrupt()
				}
				if rr.Active != nil {
					deadline, err := remoteCollectionDeadline(rr.Active)
					if err != nil || deadline == nil || !r.Expires.Equal(*deadline) {
						return nil, usage, nil, artifactCorrupt()
					}
				}
			} else {
				return nil, usage, nil, artifactCorrupt()
			}
			if exists {
				if o.Origin != "remote_result" || o.Source != r.RemoteAttemptID || o.Ref != r.Ref || o.Owner != r.Owner || present && o.JobID != rr.Job {
					return nil, usage, nil, artifactCorrupt()
				}
			} else {
				// Failed private uploads may have no installed Object, but retain
				// their original attempt and the concrete pending removal intent.
				intent := false
				for _, d := range s.Deletes {
					if d.Kind == "upload" && d.ObjectID == id && d.Owner == r.Owner && d.File == r.Part && d.Revision == r.Revision {
						intent = true
					}
				}
				if !present || rr.State < 2 || rr.State > 4 || r.State != "deleting" || !artifactFileValid(r.Part) || !intent {
					return nil, usage, nil, artifactCorrupt()
				}
			}
		} else if o, ok := s.Objects[id]; ok && o.Origin == "remote_result" {
			return nil, usage, nil, artifactCorrupt()
		}

		if r.LastSize != 0 && (r.LastSize > artifactChunkSize || r.LastOffset > r.Ack || r.LastSize != r.Ack-r.LastOffset || !artifactHex(r.LastSHA256, 64)) {
			return nil, usage, nil, artifactCorrupt()
		}
		if r.LastSize == 0 && (r.Ack != 0 || r.LastOffset != 0 || r.LastSHA256 != "") {
			return nil, usage, nil, artifactCorrupt()
		}
		if r.Part != (ArtifactFile{}) && r.Part.Device != s.Root.Volume {
			return nil, usage, nil, artifactCorrupt()
		}
		if r.State == "finalizing" {
			if o, ok := s.Objects[id]; !ok || o.State != "publishing" || o.Owner != r.Owner || o.Ref != r.Ref {
				return nil, usage, nil, artifactCorrupt()
			}
		}
		if err := put("uploads", id, r.Owner, r); err != nil {
			return nil, usage, nil, err
		}
		if r.State != "complete" {
			if err := charge(r.Owner, aPendingBytes, r.Ref.Size); err != nil {
				return nil, usage, nil, err
			}
			if err := charge(r.Owner, aPendingCount, 1); err != nil {
				return nil, usage, nil, err
			}
			if err := allocation(r.Owner, r.Ref.Size); err != nil {
				return nil, usage, nil, err
			}
		}
	}
	for id, r := range s.Objects {
		reserved := r.State == "reserved" && (r.Origin == "result" || r.Origin == "remote_result") && artifactHex(id, 32) && r.Ref.ID == id && r.Ref.SHA256 == "" && r.Ref.Size == artifactMaxSize
		if r.Ref.ID != id || !reserved && !artifactRefValid(r.Ref) || (r.State != "reserved" && r.State != "publishing" && r.State != "complete" && r.State != "deleting") || r.Expires.IsZero() || r.DeleteAfter.IsZero() || r.DeleteAfter.Before(r.Expires) {
			return nil, usage, nil, artifactCorrupt()
		}
		if r.Origin == "upload" {
			u, ok := s.Uploads[r.Source]
			if r.JobID != "" || !ok || r.Source != id || u.Owner != r.Owner || u.Ref != r.Ref {
				return nil, usage, nil, artifactCorrupt()
			}
		} else if r.Origin == "result" {
			if !artifactHex(r.JobID, 32) || !artifactHex(r.Source, 32) {
				return nil, usage, nil, artifactCorrupt()
			}
			if reserved {
				w, ok := s.Workspaces[r.Source]
				if !ok || w.ResultID != id || w.Owner != r.Owner || w.JobID != r.JobID {
					return nil, usage, nil, artifactCorrupt()
				}
			}
		} else if r.Origin == "remote_result" {
			rr, present := s.Remote[r.Source]
			if present {
				owner, err := remoteRecordOwner(s, r.Source, rr)
				if err != nil || owner != r.Owner || rr.Job != r.JobID || remoteOutputID(rr) != id {
					return nil, usage, nil, artifactCorrupt()
				}
			} else {
				return nil, usage, nil, artifactCorrupt()
			}
			if !reserved && (r.State == "reserved" || r.State == "publishing") {
				u, ok := s.Uploads[id]
				if !ok || u.RemoteAttemptID != r.Source || u.Ref != r.Ref || u.Owner != r.Owner {
					return nil, usage, nil, artifactCorrupt()
				}
			}

		} else {
			return nil, usage, nil, artifactCorrupt()
		}
		if (r.State == "complete" || r.State == "deleting") && (!artifactFileValid(r.File) || !artifactManifestValid(r.Manifest, r.Ref)) {
			return nil, usage, nil, artifactCorrupt()
		}
		if r.File != (ArtifactFile{}) && r.File.Device != s.Root.Volume {
			return nil, usage, nil, artifactCorrupt()
		}
		if (r.State == "reserved" || r.State == "publishing") && (r.File != (ArtifactFile{}) || r.Manifest != (ArtifactManifest{})) {
			return nil, usage, nil, artifactCorrupt()
		}
		if err := put("objects", id, r.Owner, r); err != nil {
			return nil, usage, nil, err
		}
		if err := charge(r.Owner, aCompletedBytes, r.Ref.Size); err != nil {
			return nil, usage, nil, err
		}
		if err := charge(r.Owner, aCompletedCount, 1); err != nil {
			return nil, usage, nil, err
		}
		if err := allocation(r.Owner, r.Ref.Size); err != nil {
			return nil, usage, nil, err
		}
		if r.State == "publishing" {
			if err := charge(r.Owner, aMetadataBytes, 1024+32); err != nil {
				return nil, usage, nil, err
			}
			if err := charge(r.Owner, aMetadataRecords, 1); err != nil {
				return nil, usage, nil, err
			}
		}
		if r.State == "reserved" {
			if err := charge(r.Owner, aMetadataBytes, 1536+32+1024+32); err != nil {
				return nil, usage, nil, err
			}
			if err := charge(r.Owner, aMetadataRecords, 2); err != nil {
				return nil, usage, nil, err
			}
		}
	}
	for id, r := range s.Grants {
		o, ok := s.Objects[id]
		if !ok || o.Owner != r.Owner || o.Ref != r.Ref || o.State != "complete" || r.Expires.IsZero() || r.Expires.Before(o.Expires) || r.Source != o.Origin || r.SourceID != o.Source {
			return nil, usage, nil, artifactCorrupt()
		}
		if r.Source == "result" || r.Source == "remote_result" {
			v, ok := s.Results[r.SourceID]
			if !ok || v.Ref != r.Ref || v.Owner != r.Owner {
				return nil, usage, nil, artifactCorrupt()
			}
		}
		if err := put("grants", id, r.Owner, r); err != nil {
			return nil, usage, nil, err
		}
	}
	for id, o := range s.Objects {
		if o.State != "complete" {
			continue
		}
		if o.Origin == "upload" {
			if _, ok := s.Grants[id]; !ok {
				return nil, usage, nil, artifactCorrupt()
			}
		}
		if o.Origin == "result" || o.Origin == "remote_result" {
			if r, ok := s.Results[o.Source]; ok {
				if r.Ref != o.Ref || r.Owner != o.Owner || r.JobID != o.JobID {
					return nil, usage, nil, artifactCorrupt()
				}
			} else if rr, ok := s.Remote[o.Source]; o.Origin == "remote_result" && ok && rr.State == 2 && remoteOutputID(rr) == id {
				// A sealed private candidate is not published until Complete.
				if _, granted := s.Grants[id]; granted {
					return nil, usage, nil, artifactCorrupt()
				}
			} else {
				r, ok := s.Expired[o.Source]
				if !ok || r.Owner != o.Owner || r.JobID != o.JobID || !s.pinned(id) && o.DeleteAfter.Before(r.Expired.Add(time.Hour)) {
					return nil, usage, nil, artifactCorrupt()
				}
			}
		}
	}
	for id, r := range s.Reads {
		if !artifactProducerValid(r.Producer) {
			return nil, usage, nil, artifactCorrupt()
		}
		o, ok := s.Objects[r.Ref.ID]
		if !ok || o.State != "complete" && !(r.Closed && o.State == "deleting") || o.Ref != r.Ref || o.Owner != r.Owner || r.Revision == 0 || r.Expires.IsZero() || r.Closed != !r.ClosedAt.IsZero() || r.Closed && !r.Closing {
			return nil, usage, nil, artifactCorrupt()
		}
		if r.Closing && r.Revision != 2 || !r.Closing && r.Revision != 1 {
			return nil, usage, nil, artifactCorrupt()
		}
		if r.RemoteAttemptID != "" {
			if !artifactHex(r.RemoteAttemptID, 32) {
				return nil, usage, nil, artifactCorrupt()
			}
			rr, present := s.Remote[r.RemoteAttemptID]
			if present {
				owner, err := remoteRecordOwner(s, r.RemoteAttemptID, rr)
				pin, pinned := s.JobInputs[rr.Job]
				if err != nil || owner != r.Owner || !pinned || pin.Ref != r.Ref || pin.Owner != r.Owner {
					return nil, usage, nil, artifactCorrupt()
				}
			} else {
				return nil, usage, nil, artifactCorrupt()
			}
		}

		if err := put("reads", id, r.Owner, r); err != nil {
			return nil, usage, nil, err
		}
		if !r.Closed {
			if err := charge(r.Owner, aDownloads, 1); err != nil {
				return nil, usage, nil, err
			}
		}
	}
	for _, table := range []struct {
		name string
		pins map[string]artifactPin
	}{{"job_inputs", s.JobInputs}, {"manual_inputs", s.ManualInputs}} {
		for id, r := range table.pins {
			o, ok := s.Objects[r.Ref.ID]
			if !ok || o.Owner != r.Owner || o.Ref != r.Ref || o.State != "complete" || !artifactHex(r.JobID, 32) || r.TaskDigest == ([32]byte{}) || table.name == "job_inputs" && id != r.JobID {
				return nil, usage, nil, artifactCorrupt()
			}
			if err := put(table.name, id, r.Owner, r); err != nil {
				return nil, usage, nil, err
			}
		}
	}
	for id, r := range s.Results {
		o, ok := s.Objects[r.Ref.ID]
		g, granted := s.Grants[r.Ref.ID]
		if !ok || !granted || o.Owner != r.Owner || o.Ref != r.Ref || o.State != "complete" || (o.Origin != "result" && o.Origin != "remote_result") || o.Source != id || o.JobID != r.JobID || g.SourceID != id || id != r.AttemptID || !artifactHex(r.JobID, 32) || r.Receipt == ([32]byte{}) {
			return nil, usage, nil, artifactCorrupt()
		}
		if _, ok := s.Expired[id]; ok {
			return nil, usage, nil, artifactCorrupt()
		}
		if err := put("results", id, r.Owner, r); err != nil {
			return nil, usage, nil, err
		}
	}
	for id, r := range s.Expired {
		if id != r.AttemptID || !artifactHex(r.JobID, 32) || !artifactHex(r.ObjectID, 32) || r.Expired.IsZero() {
			return nil, usage, nil, artifactCorrupt()
		}
		object, exists := s.Objects[r.ObjectID]
		if exists && (object.Owner != r.Owner || object.Source != id || object.JobID != r.JobID || (object.Origin != "result" && object.Origin != "remote_result")) || r.Collected && !exists {
			return nil, usage, nil, artifactCorrupt()
		}
		if err := put("result_expired", id, r.Owner, r); err != nil {
			return nil, usage, nil, err
		}
	}
	for id, r := range s.Workspaces {
		if id != r.AttemptID || !artifactHex(r.JobID, 32) || !validBinding(r.Binding) || !validInstance(r.Instance) || r.WriterClosed && r.Instance == "" || r.WriterClosed != (r.WriterReceipt != ([32]byte{})) || r.Diagnostic != artifactDiagnostic(r.Diagnostic) {
			return nil, usage, nil, artifactCorrupt()
		}
		if r.Input != (ArtifactRef{}) {
			o, ok := s.Objects[r.Input.ID]
			if !ok || o.Owner != r.Owner || o.Ref != r.Input || o.State != "complete" {
				return nil, usage, nil, artifactCorrupt()
			}
		}
		if r.ResultID != "" {
			o, ok := s.Objects[r.ResultID]
			if !ok || o.Owner != r.Owner || o.Origin != "result" || o.Source != id {
				return nil, usage, nil, artifactCorrupt()
			}
		}
		if err := put("workspaces", id, r.Owner, r); err != nil {
			return nil, usage, nil, err
		}
		if err := charge(r.Owner, aWorkspaces, 1); err != nil {
			return nil, usage, nil, err
		}
		m := s.Objects[r.Input.ID].Manifest
		if !artifactManifestValid(m, r.Input) {
			return nil, usage, nil, artifactCorrupt()
		}
		staging, files, err := artifactWorkspaceAllocation(m, s.Root.Unit)
		if err != nil {
			return nil, usage, nil, err
		}
		if err := charge(r.Owner, aAllocatedBytes, staging); err != nil {
			return nil, usage, nil, err
		}
		if err := charge(r.Owner, aFiles, files); err != nil {
			return nil, usage, nil, err
		}
		if _, done := s.Completions[id]; !done {
			if err := charge(r.Owner, aMetadataBytes, 1024+32); err != nil {
				return nil, usage, nil, err
			}
			if err := charge(r.Owner, aMetadataRecords, 1); err != nil {
				return nil, usage, nil, err
			}
		}
	}
	for id, r := range s.Completions {
		if id != r.AttemptID || !artifactHex(r.JobID, 32) || r.TaskDigest == ([32]byte{}) || r.Digest == ([32]byte{}) || !r.Outcome.valid() || r.Outcome == WorkspaceUncertain || r.OriginalTaskExit != nil && (*r.OriginalTaskExit < 0 || *r.OriginalTaskExit > 255) {
			return nil, usage, nil, artifactCorrupt()
		}
		if (r.Outcome == WorkspaceTaskExit || r.Outcome == WorkspaceCollectionFailure) != (r.OriginalTaskExit != nil) || r.Outcome == WorkspaceCollectionFailure && *r.OriginalTaskExit != 0 {
			return nil, usage, nil, artifactCorrupt()
		}
		if err := put("completions", id, r.Owner, r); err != nil {
			return nil, usage, nil, err
		}
	}
	if artifactCompletionCount(s) > 1024 {
		return nil, usage, nil, artifactCorrupt()
	}
	for id, in := range s.WorkflowInputs {
		if err := workflowInputValid(s, id, in); err != nil {
			return nil, usage, nil, err
		}
		if err := put("workflow_inputs", id, in.Owner, in); err != nil {
			return nil, usage, nil, err
		}
	}
	seen := make(map[string]bool)
	for id, r := range s.Deletes {
		key := r.Kind + ":" + r.ObjectID
		if seen[key] || !artifactHex(r.ObjectID, 32) || r.Revision == 0 || r.Kind != "workspace" && r.Workspace != nil {
			return nil, usage, nil, artifactCorrupt()
		}
		seen[key] = true
		switch r.Kind {
		case "workspace":
			if err := artifactWorkspaceDeleteValid(s, r); err != nil {
				return nil, usage, nil, err
			}
		case "object":
			o, ok := s.Objects[r.ObjectID]
			if !ok || o.State != "deleting" || o.Owner != r.Owner || o.File != r.File {
				return nil, usage, nil, artifactCorrupt()
			}
		case "upload", "finalizing":
			u, ok := s.Uploads[r.ObjectID]
			if !ok || u.State != "deleting" || u.Owner != r.Owner || u.Part != r.File || u.Revision != r.Revision {
				return nil, usage, nil, artifactCorrupt()
			}
			o, exists := s.Objects[r.ObjectID]
			if r.Kind == "finalizing" {
				if !exists || o.State != "publishing" || o.Owner != r.Owner || o.Ref != u.Ref {
					return nil, usage, nil, artifactCorrupt()
				}
			} else if exists {
				return nil, usage, nil, artifactCorrupt()
			}
		default:
			return nil, usage, nil, artifactCorrupt()
		}
		if err := put("gc", id, r.Owner, r); err != nil {
			return nil, usage, nil, err
		}
	}
	for id, o := range s.Objects {
		if o.State == "deleting" && !seen["object:"+id] {
			return nil, usage, nil, artifactCorrupt()
		}
	}
	for id, u := range s.Uploads {
		if u.State == "deleting" && !seen["upload:"+id] && !seen["finalizing:"+id] {
			return nil, usage, nil, artifactCorrupt()
		}
	}
	// Reserve a concrete future intent at original admission. A full metadata
	// budget cannot prevent cancel/delete from recording its own authority.
	reserveIntent := func(owner string) error {
		if err := charge(owner, aMetadataBytes, 2048+32); err != nil {
			return err
		}
		return charge(owner, aMetadataRecords, 1)
	}
	for id, u := range s.Uploads {
		if u.State != "complete" && !seen["upload:"+id] && !seen["finalizing:"+id] {
			if err := reserveIntent(u.Owner); err != nil {
				return nil, usage, nil, err
			}
		}
	}
	for id, o := range s.Objects {
		if !seen["object:"+id] {
			if err := reserveIntent(o.Owner); err != nil {
				return nil, usage, nil, err
			}
		}
	}
	// Every Begin prepays its own future root intent, independent of a
	// result object's GC reservation. Mark converts, never doubles, this charge.
	for id, w := range s.Workspaces {
		if !seen["workspace:"+id] {
			if err := reserveIntent(w.Owner); err != nil {
				return nil, usage, nil, err
			}
		}
	}
	rows["owners"] = make(map[string][]byte)
	for owner, u := range owners {
		if err := artifactAdd(&u[aMetadataBytes], 80+uint64(len(owner))); err != nil {
			return nil, usage, nil, err
		}
		if err := artifactAdd(&u[aMetadataRecords], 1); err != nil {
			return nil, usage, nil, err
		}
		owners[owner] = u
		rows["owners"][owner] = artifactUsageBytes(u)
		for i, n := range u {
			if err := artifactAdd(&usage[i], n); err != nil {
				return nil, usage, nil, err
			}
		}
	}
	return rows, usage, owners, nil
}

// artifactAdmission rejects growth, not recovery that is already over quota.
func artifactAdmission(before, after artifactUsage, owner bool) error {
	limits := map[int]uint64{aPendingBytes: 256 << 20, aPendingCount: 32, aCompletedBytes: 512 << 20, aCompletedCount: 128, aMetadataBytes: 2 << 20, aMetadataRecords: 4096, aDownloads: 32, aWorkspaces: 64}
	if owner {
		limits = map[int]uint64{aPendingBytes: 64 << 20, aPendingCount: 4, aCompletedBytes: 128 << 20, aCompletedCount: 64}
	}
	names := map[int]string{aPendingBytes: "pending bytes", aPendingCount: "pending uploads", aCompletedBytes: "completed bytes", aCompletedCount: "completed objects", aMetadataBytes: "metadata bytes", aMetadataRecords: "metadata records", aDownloads: "downloads", aWorkspaces: "workspaces"}
	for i := 0; i < aUsageCount; i++ {
		limit, ok := limits[i]
		if ok && after[i] > limit && after[i] > before[i] {
			return &ArtifactCapacityError{Domain: names[i], Used: before[i], Limit: limit}
		}
	}
	return nil
}

// artifactChange is the same-db metadata transaction. Shared job/prune callers
// must subtract their actual changed-value ledger from budget before calling.
// No filesystem or auth-engine work occurs while this transaction is open.
func artifactChange(tx *bolt.Tx, budget uint64, change func(*artifactState) error) (uint64, error) {
	before, err := artifactLoad(tx)
	if err != nil {
		return 0, err
	}
	after := before.clone()
	if err := change(after); err != nil {
		return 0, err
	}
	prepared, err := prepareArtifactChange(tx, before, after, nil, budget)
	if err != nil {
		return 0, err
	}
	if err := prepared.apply(tx); err != nil {
		return 0, err
	}
	return prepared.changed, nil
}

// initializeArtifacts is called only after the successor fence exists. The
// stopped InstallArtifacts caller supplies previously verified root geometry.
func initializeArtifacts(tx *bolt.Tx, root artifactRoot) error {
	if root.SchemaFloor != artifactFormat || schemaNumber(tx) != schemaVersion || tx.Bucket(artifactsBucket) != nil {
		return ErrArtifactConflict
	}
	b, err := tx.CreateBucket(artifactsBucket)
	if err != nil {
		return err
	}
	for _, t := range artifactTables {
		if _, err := b.CreateBucket([]byte(t.name)); err != nil {
			return err
		}
	}
	raw, err := json.Marshal(root)
	if err != nil {
		return err
	}
	if err := b.Put([]byte("state"), raw); err != nil {
		return err
	}
	u := artifactRootUsage(root)
	if err := b.Put([]byte("usage"), artifactUsageBytes(u)); err != nil {
		return err
	}
	_, err = artifactLoad(tx)
	return err
}
