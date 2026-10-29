package main

import (
	"os"
	"path/filepath"
)

// localDirs returns the directories holding the local agent identities for
// host, skygo-ssh/agents/HOST under the user config directory, and the
// agent builds, skygo-ssh/build under the user cache directory.
func localDirs(host string) (identities, builds string, err error) {
	config, err := os.UserConfigDir()
	if err != nil {
		return "", "", err
	}
	cache, err := os.UserCacheDir()
	if err != nil {
		return "", "", err
	}
	return filepath.Join(config, "skygo-ssh", "agents", host), filepath.Join(cache, "skygo-ssh", "build"), nil
}
