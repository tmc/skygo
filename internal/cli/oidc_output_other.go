//go:build !linux && !darwin

package cli

import (
	"errors"
	"os"
)

var errTokenOutputPlatform = errors.New("OIDC sign-in output requires Linux or Darwin")

// tokenOutput is unavailable here: publication needs descriptor-relative
// directory operations, so sign-in fails before it opens a browser.
type tokenOutput struct{}

func openTokenOutput(string, bool) (*tokenOutput, error) { return nil, errTokenOutputPlatform }

func (*tokenOutput) publish(string) error { return errTokenOutputPlatform }

func (*tokenOutput) Close() error { return nil }

func openOIDCCA(string) (*os.File, error) { return nil, errTokenOutputPlatform }
