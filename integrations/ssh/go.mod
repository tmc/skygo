module github.com/tmc/skygo/integrations/ssh

go 1.24.1

require (
	connectrpc.com/connect v1.18.1
	github.com/spf13/cobra v1.9.1
	github.com/tmc/skygo v0.0.0
	golang.org/x/crypto v0.48.0
)

require (
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/spf13/pflag v1.0.6 // indirect
	go.etcd.io/bbolt v1.3.11 // indirect
	golang.org/x/sys v0.41.0 // indirect
	google.golang.org/protobuf v1.36.6 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/tmc/skygo => ../..
