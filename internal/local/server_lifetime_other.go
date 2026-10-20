//go:build !linux

package local

// CaptureServerLifetime reports unavailable identity on unsupported platforms.
// It opens nothing. Live artifact transfer remains available; automatic stopped
// recovery cannot use this zero identity.
func CaptureServerLifetime() (ServerLifetime, error) {
	return ServerLifetime{}, ErrServerLifetimeUnavailable
}
