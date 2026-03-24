FROM golang:1.24 AS builder
ARG TARGETOS
ARG TARGETARCH

WORKDIR /workspace
COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} \
    go build -a -o queue ./cmd/queue

FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=builder /workspace/queue .
USER 65532:65532

ENTRYPOINT ["/queue"]
