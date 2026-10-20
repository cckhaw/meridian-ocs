SERVICES := $(notdir $(wildcard services/ocs-*))
TOOLS    := $(notdir $(wildcard tools/ocs-*))
MODULES  := pkg $(addprefix services/,$(SERVICES)) $(addprefix tools/,$(TOOLS))

.PHONY: build test vet fmt proto docker lint-proto proto-tools proto-check stack-up stack-smoke stack-down loadtest chaos
build:
	@for m in $(MODULES); do (cd $$m && go build ./...) || exit 1; done
test:
	@for m in $(MODULES); do (cd $$m && go test -race -count=1 ./...) || exit 1; done
vet:
	@for m in $(MODULES); do (cd $$m && go vet ./...) || exit 1; done
fmt:
	@test -z "$$(gofmt -l pkg services tools)" || (gofmt -l pkg services tools; exit 1)
# Generated code is committed (pkg/gen); `make proto` regenerates it and
# `make proto-check` fails if the committed copy is stale. Needs buf and the
# two plugins on PATH: `make proto-tools` installs them (pinned versions).
proto:
	cd api/proto && buf generate
lint-proto:
	cd api/proto && buf lint
proto-tools:
	go install github.com/bufbuild/buf/cmd/buf@v1.47.2
	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.34.2
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.4.0
proto-check: lint-proto proto
	@git diff --exit-code -- pkg/gen || (echo "pkg/gen is stale: run make proto and commit"; exit 1)
docker:
	@for s in $(SERVICES); do docker build -f build/Dockerfile --build-arg SERVICE=$$s -t meridian/$$s:dev . || exit 1; done

# The local development stack (docker-compose.yml): Redis primary + replica,
# Redpanda and the services, authentication off, ports bound to 127.0.0.1.
stack-up:
	docker compose up -d --build
stack-smoke:
	dev/smoke.sh
stack-down:
	docker compose down -v

# Load test against the running local stack (make stack-up first). A short run with objectives that fail the
# command when missed; tune with LOAD_ARGS, e.g. make loadtest LOAD_ARGS="-rate 1000 -duration 60s".
LOAD_ARGS ?= -rate 200 -duration 20s -warmup 5s -gateway-admin http://127.0.0.1:9080
loadtest:
	cd tools/ocs-loadgen && go run . $(LOAD_ARGS)

# Failure injection under load (stops, freezes and kills containers of the local stack; development only).
chaos:
	dev/chaos.sh all
