module github.com/tmc/skygo/integrations/ssh

go 1.24.1

require (
	connectrpc.com/connect v1.18.1
	github.com/tmc/skygo v0.0.0
	golang.org/x/crypto v0.48.0
)

require (
	golang.org/x/sys v0.41.0 // indirect
	google.golang.org/protobuf v1.36.6 // indirect
)

replace github.com/tmc/skygo => ../..
