FROM golang:1.25 AS builder

WORKDIR /workspace
# Copy both gpuscale and pkg/provider (needed by go.mod replace directive)
COPY gpuscale/go.mod gpuscale/go.sum gpuscale/
COPY pkg/ pkg/
COPY go.work go.work.sum ./
WORKDIR /workspace/gpuscale
RUN go mod download

COPY gpuscale/ .
RUN CGO_ENABLED=0 GOOS=linux go build -a -o gpuscale-controller ./cmd/controller/

FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=builder /workspace/gpuscale-controller .
USER 65532:65532
ENTRYPOINT ["/gpuscale-controller"]
