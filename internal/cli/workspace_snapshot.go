package cli

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// resolvedWorkspacePath expands only the current user's home shorthand.
func resolvedWorkspacePath(name string) (string, error) {
	if name == "" {
		return "", errors.New("workspace path is empty")
	}
	if name == "~" || strings.HasPrefix(name, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		if name == "~" {
			name = home
		} else {
			name = home + string(filepath.Separator) + strings.TrimPrefix(name, "~/")
		}
	} else if strings.HasPrefix(name, "~") {
		return "", errors.New("workspace path supports only current-user home expansion")
	}
	return cleanWorkspacePath(name)
}

// cleanWorkspacePath inspects the original spelling before cleaning it. It
// does not expand home shorthand or environment variables.
func cleanWorkspacePath(name string) (string, error) {
	if !filepath.IsAbs(name) {
		cwd, err := os.Getwd()
		if err != nil {
			return "", err
		}
		name = cwd + string(filepath.Separator) + name
	}
	// Inspect the original spelling before Clean can erase a symlink/.. pair.
	prefix := string(filepath.Separator)
	for _, part := range strings.Split(name, string(filepath.Separator)) {
		if part == "" || part == "." {
			continue
		}
		prefix = prefix + string(filepath.Separator) + part
		info, err := os.Lstat(prefix)
		if err == nil && info.Mode()&os.ModeSymlink != 0 {
			return "", errors.New("workspace path contains a symlink")
		}
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
	}
	return filepath.Abs(name)
}

type workspacePathPart struct {
	path string
	info os.FileInfo
}

// workspacePath checks every component. The caller's trusted, quiescent-path
// premise excludes hostile transient ABA; these observations do not prove it.
func workspacePath(path string, create bool, boundary string) ([]workspacePathPart, error) {
	return workspacePathWithSync(path, create, boundary, syncWorkspaceDirectory)
}

// The concrete local parameter permits a modeled failure at the directory Sync
// boundary. Production always supplies syncWorkspaceDirectory; no global hook
// or alternate durability receipt exists.
func workspacePathWithSync(path string, create bool, boundary string, syncParent func(string) error) ([]workspacePathPart, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	var names []string
	for p := path; ; p = filepath.Dir(p) {
		names = append(names, p)
		if filepath.Dir(p) == p {
			break
		}
	}
	var parts []workspacePathPart
	for i := len(names) - 1; i >= 0; i-- {
		name := names[i]
		private := boundary != "" && (name == boundary || strings.HasPrefix(name, boundary+string(filepath.Separator)))
		info, err := os.Lstat(name)
		if errors.Is(err, os.ErrNotExist) && create {
			if !private {
				return nil, errors.New("workspace state parent must already exist")
			}
			err = os.Mkdir(name, 0700)
			if err == nil {
				info, err = os.Lstat(name)
			}
		}
		if err != nil {
			return nil, fmt.Errorf("inspect workspace directory: %w", err)
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return nil, errors.New("workspace directory component is not a real directory")
		}
		if boundary != "" {
			if err := workspacePermissions(info, private, true); err != nil {
				return nil, err
			}
		}
		// Reestablish every selected managed link, including an empty directory
		// left by an earlier failed parent Sync. Existing is not a receipt.
		if private {
			if err := syncParent(filepath.Dir(name)); err != nil {
				return nil, err
			}
		}
		parts = append(parts, workspacePathPart{name, info})
	}
	return parts, nil
}

func recheckWorkspacePath(parts []workspacePathPart) error {
	for _, p := range parts {
		info, err := os.Lstat(p.path)
		if err != nil {
			return err
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || !os.SameFile(p.info, info) || p.info.Mode() != info.Mode() {
			return errors.New("workspace directory changed")
		}
	}
	return nil
}

// snapshotWorkspace consumes a checked root using the shared archive policy.
// Writers must be quiescent and the path trusted throughout the operation.
func snapshotWorkspace(directory, journal string) ([]byte, string, error) {
	path, err := resolvedWorkspacePath(directory)
	if err != nil {
		return nil, "", err
	}
	journal, err = filepath.Abs(journal)
	if err != nil {
		return nil, "", err
	}
	rel, err := filepath.Rel(path, journal)
	if err != nil {
		return nil, "", err
	}
	if rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))) {
		return nil, "", errors.New("workspace state must be outside the workspace")
	}
	parts, err := workspacePath(path, false, "")
	if err != nil {
		return nil, "", err
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, "", err
	}
	info, err := root.Stat(".")
	if err == nil && !os.SameFile(parts[len(parts)-1].info, info) {
		err = errors.New("workspace root changed")
	}
	if err == nil {
		err = recheckWorkspacePath(parts)
	}
	if err != nil {
		closeErr := root.Close()
		if closeErr != nil {
			closeErr = &snapshotCloseError{root: root, err: closeErr}
		}
		return nil, "", errors.Join(err, closeErr)
	}
	return snapshotRoot(root)
}

// A new managed directory must be durable in its parent before any RPC can
// rely on its journal. Preserve a failed concrete Close owner just as for files.
func syncWorkspaceDirectory(name string) error {
	f, err := os.OpenFile(name, os.O_RDONLY|workspaceNoFollow, 0)
	if err != nil {
		return err
	}
	syncErr := f.Sync()
	closeErr := f.Close()
	if closeErr != nil {
		closeErr = &workspaceCloseError{file: f, err: closeErr}
	}
	return errors.Join(syncErr, closeErr)
}

// The default cache may create one leaf below an existing trusted parent.
// Sync that link on every open: a preexisting leaf may be a failed prior create.
func prepareWorkspaceCache(cache string, syncParent func(string) error) error {
	parent := filepath.Dir(cache)
	if parent == cache || filepath.Dir(parent) == parent {
		return errors.New("workspace cache requires a non-root parent")
	}
	if _, err := workspacePathWithSync(parent, false, cache, syncParent); err != nil {
		return err
	}
	info, err := os.Lstat(cache)
	if errors.Is(err, os.ErrNotExist) {
		if err = os.Mkdir(cache, 0700); err != nil {
			return err
		}
		info, err = os.Lstat(cache)
	}
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("workspace cache is not a real directory")
	}
	if err = workspacePermissions(info, false, true); err != nil {
		return err
	}
	if err = workspaceCacheOwner(info); err != nil {
		return err
	}
	return syncParent(parent)
}
