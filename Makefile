SERVICES := $(notdir $(wildcard services/ocs-*))
MODULES  := pkg $(addprefix services/,$(SERVICES))

.PHONY: build test vet fmt proto docker lint-proto proto-tools proto-check
build:
	@for m in $(MODULES); do (cd $$m && go build ./...) || exit 1; done
test:
	@for m in $(MODULES); do (cd $$m && go test -race -count=1 ./...) || exit 1; done
vet:
	@for m in $(MODULES); do (cd $$m && go vet ./...) || exit 1; done
fmt:
	@test -z "$$(gofmt -l pkg services)" || (gofmt -l pkg services; exit 1)
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
