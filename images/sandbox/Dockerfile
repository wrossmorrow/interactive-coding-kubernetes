# Thin interactive sandbox image layered directly on agentrunner's multi-arch
# toolchain images (agent, python, golang, rust, java17, java21).
ARG BASE_IMAGE=ghcr.io/wrossmorrow/agentrunner-agent:latest

FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS pi-sessiond-build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY pkg/ ./pkg/
COPY cmd/pi-sessiond/ ./cmd/pi-sessiond/
RUN CGO_ENABLED=0 GOOS="${TARGETOS:-linux}" GOARCH="${TARGETARCH:-$(go env GOARCH)}" \
    go build -trimpath -ldflags="-s -w" -o /pi-sessiond ./cmd/pi-sessiond

FROM ${BASE_IMAGE}
USER root
COPY --from=pi-sessiond-build /pi-sessiond /usr/local/bin/pi-sessiond
COPY ext/session-attribution/ /opt/pi-ext/session-attribution/
COPY agents/base.md /opt/agents/base.md
COPY agents/analyst.md /opt/agents/analyst.md
RUN mkdir -p /work/repo \
    && chown -R node:node /work /opt/pi-ext /opt/agents \
    && git config --system --add credential.https://github.com.helper "" \
    && git config --system --add credential.https://github.com.helper '!/usr/local/bin/gh auth git-credential'

ENV PI_EXTENSION_PATH=/opt/pi-ext/session-attribution/index.ts \
    PI_AGENTS_MD=/opt/agents/base.md \
    PI_WORK_DIR=/work/repo \
    PI_SESSIOND_ADDR=0.0.0.0:8787

USER node
WORKDIR /work/repo
EXPOSE 8787
ENTRYPOINT ["/usr/local/bin/pi-sessiond"]
