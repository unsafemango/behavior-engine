# Shared multi-stage build for all three behavior-engine binaries.
# docker-compose passes CMD_PATH (ingestion-api | profile-consumer |
# strategy-api) as a build arg to select which one gets built.

FROM golang:1.25-alpine AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG CMD_PATH
RUN test -n "$CMD_PATH" || (echo "CMD_PATH build arg is required" && false)
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${CMD_PATH}

FROM alpine:3.20
# Reuse ca-certificates and a non-root user setup without touching the
# network at build time (apk's package index isn't reachable from
# inside a build container in some sandboxed/offline CI environments) --
# the certs already exist in the golang:1.25-alpine build stage.
RUN adduser -D -u 10001 app
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
USER app
COPY --from=build /out/app /app
ENTRYPOINT ["/app"]
