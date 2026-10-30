module github.com/tmc/skygo/integrations/ec2

go 1.25.0

require (
	connectrpc.com/connect v1.18.1
	github.com/aws/aws-sdk-go-v2 v1.41.7
	golang.org/x/tools v0.26.0
	google.golang.org/protobuf v1.36.6
)

require (
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/spf13/cobra v1.9.1 // indirect
	github.com/spf13/pflag v1.0.6 // indirect
	go.etcd.io/bbolt v1.3.11 // indirect
	golang.org/x/crypto v0.48.0 // indirect
	golang.org/x/sys v0.41.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

require (
	github.com/aws/smithy-go v1.25.1 // indirect
	github.com/tmc/skygo v0.0.0
	github.com/tmc/skygo/integrations/ssh v0.0.0
	rsc.io/script v0.0.2
)

replace github.com/tmc/skygo => ../..

replace github.com/tmc/skygo/integrations/ssh => ../ssh
