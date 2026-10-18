# Local server and portable client. For Kubernetes, use integrations/kubernetes/Dockerfile.
# Supply a digest-pinned Go builder for release builds.
ARG GO_IMAGE=golang:1.24.1
FROM ${GO_IMAGE} AS build
ENV GOWORK=off
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /skygo ./cmd/skygo

FROM scratch
COPY --from=build /skygo /skygo
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
USER 65532:65532
ENTRYPOINT ["/skygo"]
CMD ["server", "--data-dir=/data/state", "--listen=0.0.0.0:8080"]
