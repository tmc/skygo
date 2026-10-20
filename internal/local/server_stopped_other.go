//go:build !linux

package local

// ObserveServerStopped reports unavailable observation on unsupported platforms.
// It opens nothing and never authorizes stopped-reader settlement.
func ObserveServerStopped(current, original ServerLifetime) (bool, error) {
	return false, ErrServerLifetimeUnavailable
}
