# Build any service: docker build -f build/Dockerfile --build-arg SERVICE=ocs-rating-engine .
FROM golang:1.22-bookworm AS build
ARG SERVICE
WORKDIR /src
COPY . .
RUN test -n "$SERVICE" && cd services/$SERVICE && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/service .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/service /service
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/service"]
