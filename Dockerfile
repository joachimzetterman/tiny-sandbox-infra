# One image for every role. The build stage has the Go toolchain, the final
# image has only the static binary, so 40 containers stay small.
# The exact version go.mod asks for. A floating tag can resolve to an older
# cached image, and the build then stops with "go.mod requires go >= 1.27.4".
FROM golang:1.27.4-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /tsi ./cmd/tsi

FROM scratch
COPY --from=build /tsi /tsi
# Not root. Only launch overrides this, because it needs the Docker socket.
USER 65532:65532
ENTRYPOINT ["/tsi"]
