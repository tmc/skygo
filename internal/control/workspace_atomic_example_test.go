package control

import (
	"errors"
	"fmt"

	"github.com/tmc/skygo/internal/local"
)

func ExampleConfig_serverLifetime() {
	// Startup captures self once, outside Control. A capture Close error must be
	// returned with its original resource frame; it cannot become zero identity.
	configure := func(original local.ServerLifetime, captureErr error) (Config, error) {
		var uncertain *local.ServerLifetimeCloseError
		if errors.As(captureErr, &uncertain) {
			return Config{}, captureErr
		}
		if captureErr != nil && !errors.Is(captureErr, local.ErrServerLifetimeUnavailable) {
			return Config{}, captureErr
		}
		return Config{ServerLifetime: original}, nil
	}
	// Ordinary positively settled unavailable capture preserves live transfers.
	config, err := configure(local.ServerLifetime{}, local.ErrServerLifetimeUnavailable)
	fmt.Println(config.ServerLifetime.PID, err)
	// Output: 0 <nil>
}
