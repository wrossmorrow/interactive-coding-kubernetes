FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY pkg/ ./pkg/
COPY ui/ ./ui/
COPY cmd/session-hub/ ./cmd/session-hub/
RUN CGO_ENABLED=0 GOOS="${TARGETOS:-linux}" GOARCH="${TARGETARCH:-$(go env GOARCH)}" \
    go build -trimpath -ldflags="-s -w" -o /session-hub ./cmd/session-hub

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /session-hub /usr/local/bin/session-hub
EXPOSE 8787
ENTRYPOINT ["/usr/local/bin/session-hub"]
