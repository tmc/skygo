module github.com/tmc/skygo/integrations/skycompat

go 1.24.1

require (
	connectrpc.com/connect v1.18.1
	github.com/tmc/skygo v0.0.0
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/kr/text v0.2.0 // indirect
	google.golang.org/protobuf v1.36.6 // indirect
)

replace github.com/tmc/skygo => ../..
