package store

import (
	"errors"

	bolt "go.etcd.io/bbolt"
)

// InstallArtifacts installs the stopped installation's companion artifact root.
// The caller must first obtain ID, Unit and Volume from the concrete Files
// initializer and its verified RootFacts. These values identify an existing
// owned root; this function never creates, probes, adopts or rebinds its files.
// Enabled must be true. The server must be stopped: Open holds the same exclusive
// database lock used by the server. Existing matching configuration is unchanged;
// any different installed root is refused. A dormant database may migrate to the
// current schema through Open, without changing existing accepted job bytes.
func InstallArtifacts(directory string, installation ArtifactInstallation) (out ArtifactInstallation, err error) {
	if directory == "" || !validArtifactInstallation(installation) {
		return out, errors.New("invalid artifact installation")
	}
	s, e := Open(directory)
	if e != nil {
		return out, e
	}
	defer func() { err = errors.Join(err, s.Close()) }()
	return s.InstallArtifacts(installation)
}

// InstallArtifacts installs verified companion-root facts while retaining this
// Store's existing exclusive lock. The caller must stop all prior writers and
// obtain these facts from the concrete Files initializer and RootFacts before
// calling this method. No filesystem operation runs in the transaction.
// Existing matching configuration is unchanged; a different root is refused.
// This method neither closes the Store nor proves filesystem ownership.
func (s *Store) InstallArtifacts(installation ArtifactInstallation) (out ArtifactInstallation, err error) {
	if !validArtifactInstallation(installation) {
		return out, errors.New("invalid artifact installation")
	}
	err = s.db.Update(func(tx *bolt.Tx) error {
		if err := validateSnapshot(tx); err != nil {
			return err
		}
		if tx.Bucket(artifactsBucket) != nil {
			st, e := artifactLoad(tx)
			if e != nil {
				return e
			}
			if st.Root.ID != installation.ID || st.Root.Unit != installation.Unit || st.Root.Volume != installation.Volume {
				return ErrArtifactConflict
			}
		} else {
			root := artifactRoot{Layout: 1, SchemaFloor: schemaVersion, Policy: "default-v1", ID: installation.ID, Unit: installation.Unit, Volume: installation.Volume}
			// Root geometry was bounded before this transaction. The fixed marker and root
			// allowance is two files and 2*Unit, not a physical usage sample.
			if e := initializeArtifacts(tx, root); e != nil {
				return e
			}
		}
		if e := validateSnapshot(tx); e != nil {
			return e
		}
		out = installation
		return nil
	})
	return
}

func validArtifactInstallation(installation ArtifactInstallation) bool {
	return installation.Enabled && artifactHex(installation.ID, 32) && installation.Unit >= 512 && installation.Unit <= 65536 && installation.Unit&(installation.Unit-1) == 0 && installation.Volume != 0
}
