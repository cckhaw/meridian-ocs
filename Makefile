SERVICES := $(notdir $(wildcard services/ocs-*))
MODULES  := pkg $(addprefix services/,$(SERVICES))

.PHONY: build test vet fmt proto docker lint-proto
build:
	@for m in $(MODULES); do (cd $$m && go build ./...) || exit 1; done
test:
	@for m in $(MODULES); do (cd $$m && go test -race -count=1 ./...) || exit 1; done
vet:
	@for m in $(MODULES); do (cd $$m && go vet ./...) || exit 1; done
fmt:
	@test -z "$$(gofmt -l pkg services)" || (gofmt -l pkg services; exit 1)
proto:
	cd api/proto && buf generate
lint-proto:
	cd api/proto && buf lint
docker:
	@for s in $(SERVICES); do docker build -f build/Dockerfile --build-arg SERVICE=$$s -t meridian/$$s:dev . || exit 1; done
