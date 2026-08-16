FROM golang:1.25 AS builder

WORKDIR /workspace

# Dependencies first, so a source-only change does not re-download the module cache.
COPY go.mod go.sum ./
RUN go mod download

COPY api/ api/
COPY cmd/ cmd/
COPY internal/ internal/
COPY pkg/ pkg/

ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} \
    go build -a -trimpath -ldflags="-s -w" -o gpuscale-controller ./cmd/controller/

FROM gcr.io/distroless/static:nonroot

# Binds the GHCR package to this repository. Without it the package stays
# unlinked, and another repository that pushes the same name can claim it.
LABEL org.opencontainers.image.source="https://github.com/munhq/gpuscale"

WORKDIR /
COPY --from=builder /workspace/gpuscale-controller .
USER 65532:65532
ENTRYPOINT ["/gpuscale-controller"]
