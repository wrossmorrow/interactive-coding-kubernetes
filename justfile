image_repository := "ghcr.io/wrossmorrow"
image_name_prefix := "k8s-agents"

# list targets and exit
default:
    @just --list

# format all Go files
fmt:
    gofmt -w .

# fail if any Go files are not formatted
fmt-check:
    test -z "$(gofmt -l .)"

# run go vet across all packages
vet:
    go vet ./...

# build all packages and binaries
build:
    go build ./...

# run all tests with race detection and shuffling
test:
    go test -race -shuffle=on -count=1 ./...

# run all tests verbosely
test-v:
    go test -race -shuffle=on -count=1 -v ./...

alias validate := check

# run all formatting, static, build, and test validations
check: fmt-check vet build test

# run session-hub locally with dirStore and local pi-sessiond backed by fake-pi.mjs
dev port="8787":
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p .dev/bin .dev/store .dev/work
    go build -o .dev/bin/pi-sessiond ./cmd/pi-sessiond
    go build -o .dev/bin/session-hub ./cmd/session-hub
    exec .dev/bin/session-hub \
        --addr "127.0.0.1:{{ port }}" \
        --store-dir .dev/store \
        --work-dir .dev/work \
        --sessiond-bin "$(pwd)/.dev/bin/pi-sessiond" \
        --pi-bin node \
        --pi-args "$(pwd)/testdata/fake-pi.mjs"

# build a thin interactive sandbox image layered on an agentrunner toolchain image
containerize-sandbox toolchain="agent" tag="latest" agentrunner_tag="latest" *args="":
    docker build -f images/sandbox/Dockerfile \
        --build-arg "BASE_IMAGE={{ image_repository }}/agentrunner-{{ toolchain }}:{{ agentrunner_tag }}" \
        {{ args }} \
        -t "{{ image_repository }}/{{ image_name_prefix }}-sandbox-{{ toolchain }}:{{ tag }}" .

# build the session-hub control plane image
containerize-hub tag="latest" *args="":
    docker build -f images/session-hub/Dockerfile \
        {{ args }} \
        -t "{{ image_repository }}/{{ image_name_prefix }}-session-hub:{{ tag }}" .

# open a zero-ingress local kubeconfig tunnel to svc/session-hub on 127.0.0.1
ui port="8787" context="" *args="":
    go run ./cmd/k8s-agents ui --port "{{ port }}" {{ if context != "" { "--context " + context } else { "" } }} {{ args }}
