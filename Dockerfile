FROM golang:1.25-bookworm AS builder

ARG versionflags

WORKDIR /app

RUN apt-get update && apt-get upgrade -y && rm -rf /var/lib/apt/lists/*

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -v -a -tags netgo -ldflags="-extldflags '-static' -s -w $versionflags" -o osctl cmd/main.go

FROM debian:bookworm-slim

WORKDIR /app

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get upgrade -y && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        wget \
        dnsutils \
        iputils-ping \
        vim \
        nano \
        jq \
        lsof \
        net-tools \
        procps \
        tzdata \
        traceroute \
        mtr-tiny \
        && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/osctl /app/osctl

RUN chmod +x /app/osctl

ENV PATH="/app:${PATH}"

RUN mkdir -p /etc/bash_completion.d
RUN /app/osctl completion bash > /etc/bash_completion.d/osctl

RUN echo '[ -f /etc/bash_completion ] && . /etc/bash_completion' >> /etc/bash.bashrc

CMD ["/app/osctl"]