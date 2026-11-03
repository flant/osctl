FROM golang:1.25-bookworm AS builder

ARG versionflags

WORKDIR /app

RUN apt-get update && apt-get upgrade -y && rm -rf /var/lib/apt/lists/*

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -v -a -tags netgo -ldflags="-extldflags '-static' -s -w $versionflags" -o osctl cmd/main.go

FROM gcr.io/distroless/static-debian13

WORKDIR /app

COPY --from=builder /app/osctl /app/osctl

USER nonroot:nonroot

ENTRYPOINT ["/app/osctl"]