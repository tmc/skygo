TOOLS := $(CURDIR)/.tools
export GOWORK := off
export PATH := $(TOOLS):$(PATH)

.PHONY: tools generate check test race build

tools:
	GOBIN=$(TOOLS) go install github.com/bufbuild/buf/cmd/buf@v1.50.0
	GOBIN=$(TOOLS) go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.6
	GOBIN=$(TOOLS) go install connectrpc.com/connect/cmd/protoc-gen-connect-go@v1.18.1

generate:
	buf lint
	buf generate

check:
	buf lint
	buf build -o /dev/null
	go vet ./...
	cd integrations/kubernetes && go vet ./...
	cd integrations/skycompat && go vet ./...

test:
	go test ./...
	cd integrations/kubernetes && go test ./...
	cd integrations/skycompat && go test ./...

race:
	go test -race ./...
	cd integrations/kubernetes && go test -race ./...
	cd integrations/skycompat && go test -race ./...

build:
	CGO_ENABLED=0 go build -o bin/skygo ./cmd/skygo
	cd integrations/kubernetes && CGO_ENABLED=0 go build -o ../../bin/skygo-kubernetes ./cmd/skygo-kubernetes
	cd integrations/skycompat && CGO_ENABLED=0 go build ./...
	cd integrations/skycompat && CGO_ENABLED=0 go build -o ../../bin/skycompat ./cmd/skycompat
