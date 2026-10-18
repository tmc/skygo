# Build from the repository root with -f integrations/kubernetes/Dockerfile.
# Supply a digest-pinned Go builder for release builds.
ARG GO_IMAGE=golang:1.24.1
FROM ${GO_IMAGE} AS build
ENV GOWORK=off
WORKDIR /src
COPY go.mod go.sum ./
COPY integrations/kubernetes/go.mod integrations/kubernetes/go.sum ./integrations/kubernetes/
RUN go mod download
WORKDIR /src/integrations/kubernetes
RUN go mod download
COPY . /src
RUN CGO_ENABLED=0 go build -trimpath -o /skygo-kubernetes ./cmd/skygo-kubernetes

FROM scratch
COPY --from=build /skygo-kubernetes /skygo-kubernetes
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
USER 65532:65532
ENTRYPOINT ["/skygo-kubernetes"]
CMD ["server", "--data-dir=/data/state", "--listen=0.0.0.0:8080", "--disable-local", "--namespace=skygo"]
