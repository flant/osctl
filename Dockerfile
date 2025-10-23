FROM golang:1.25-bullseye as builder

ARG versionflags

WORKDIR /app

COPY . .

RUN CGO_ENABLED=0 go build -v -a -tags netgo -ldflags="-extldflags '-static' -s -w $versionflags" -o build/curator-go cmd/main.go


FROM debian:bullseye-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -qy --no-install-recommends \
        ca-certificates

COPY --from=builder /src/build/curator-go /usr/local/bin/curator-go

CMD [ "/usr/local/bin/curator-go" ]