# syntax=docker/dockerfile:1@sha256:ecfaec9ed6d810b56388c508f4121597bfbba70d41a6dfeee4d8cad5f295fc32

# Multi-architecture build input pinned to the manifest list for Go 1.25.12.
FROM --platform=$BUILDPLATFORM docker.io/library/golang:1.25-bookworm@sha256:6359592445455f2dbe2412bed411336035bc019a50017720d77454ffdd6d0f82 AS build

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=Snapshot

WORKDIR /src
COPY . .

RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -mod=vendor -trimpath \
    -ldflags="-s -w -extldflags '-static' -X github.com/scylladb/scylla-manager/v3/pkg.version=${VERSION}" \
    -o /out/scylla-manager ./pkg/cmd/scylla-manager
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -mod=vendor -trimpath \
    -ldflags="-s -w -extldflags '-static' -X github.com/scylladb/scylla-manager/v3/pkg.version=${VERSION}" \
    -o /out/sctool ./pkg/cmd/sctool
RUN mkdir -p /out/state /out/config

# Runtime pinned to a multi-architecture distroless manifest. It contains CA
# roots but no shell/package manager, and runs as uid/gid 65532 by default.
FROM gcr.io/distroless/static-debian12:nonroot@sha256:1b7b9f0f0e0a1d2155f531db587cc48ec26aaf97ab64364225f5bf18a054e66a

COPY --from=build --chown=65532:65532 /out/scylla-manager /usr/bin/scylla-manager
COPY --from=build --chown=65532:65532 /out/sctool /usr/bin/sctool
COPY --from=build --chown=65532:65532 /out/state /var/lib/scylla-manager
COPY --from=build --chown=65532:65532 /src/dist/etc/scylla-manager.yaml /etc/scylla-manager/scylla-manager.yaml

ENV HOME=/var/lib/scylla-manager
USER 65532:65532
EXPOSE 5443 5090
ENTRYPOINT ["/usr/bin/scylla-manager"]
