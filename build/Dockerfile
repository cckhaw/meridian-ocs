# Build any service: docker build -f build/Dockerfile --build-arg SERVICE=ocs-rating-engine .
# Works on amd64 and arm64 (Apple Silicon) hosts: the image is built for the host architecture.
FROM golang:1.22-bookworm AS build
ARG SERVICE
WORKDIR /src
COPY . .
# The cache mounts keep downloaded modules and compiled packages between builds.
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    test -n "$SERVICE" && cd services/$SERVICE && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/service . && \
    mkdir -p /out/data && chown 65532:65532 /out/data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/service /service
# A volume mounted at /data starts out owned by the non-root user (mediation writes there).
COPY --from=build --chown=65532:65532 /out/data /data
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/service"]
