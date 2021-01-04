GOPATH:=$(shell go env GOPATH)
PULSAR_CERT:=cert+data

GO_SOURCES := $(wildcard *.go)
GO_SOURCES += $(shell find . -type f -name "*.go")

GOFMT ?= gofmt -s

ifeq ($(filter $(TAGS_SPLIT),bindata),bindata)
	GO_SOURCES += $(BINDATA_DEST)
endif

GO_SOURCES_OWN := $(filter-out vendor/%, $(GO_SOURCES))

PROTO_SRC_DIR := ${PWD}/proto
PROTO_DST_DIR := ${PWD}/proto

.PHONY: proto
proto:
	mkdir -p ${PROTO_DST_DIR} && protoc -I=${PROTO_SRC_DIR} --go_out=plugins=grpc:${PROTO_DST_DIR} ${PROTO_SRC_DIR}/service.proto

.PHONY: build
build: proto
	go build -o srv *.go

.PHONY: test
test:
	go test -v ./... -cover

.PHONY: docker
docker: proto gen-mocks
	docker build . -t ms.notify:alpine

gen-mocks:
	go generate ./...

local: proto gen-mocks
	go run main.go

tools:
	go get golang.org/x/tools/cmd/goimports
	go get github.com/kisielk/errcheck
	go get golang.org/x/lint/golint
	go get github.com/axw/gocov/gocov
	go get github.com/matm/gocov-html
	go get github.com/tools/godep
	go get github.com/mitchellh/gox

lint:
	@hash golangci-lint > /dev/null 2>&1; if [ $$? -ne 0 ]; then \
		export BINARY="golangci-lint"; \
		curl -sfL https://install.goreleaser.com/github.com/golangci/golangci-lint.sh | sh -s -- -b $(GOPATH)/bin v1.31.0; \
	fi
	golangci-lint run --timeout 5m

vet:
	go vet -v ./...

fmt:
	gofmt -w .

fmt-check:
	@diff=$$($(GOFMT) -d $(GO_SOURCES_OWN)); \
	if [ -n "$$diff" ]; then \
		echo "Please run 'make fmt' and commit the result:"; \
		echo "$${diff}"; \
		exit 1; \
	fi;

errors:
	errcheck -ignoretests -blank ./...

coverage:
	gocov test ./... > $(CURDIR)/coverage.out 2>/dev/null
	gocov report $(CURDIR)/coverage.out
	if test -z "$$CI"; then \
	  gocov-html $(CURDIR)/coverage.out > $(CURDIR)/coverage.html; \
	  if which open &>/dev/null; then \
	    open $(CURDIR)/coverage.html; \
	  fi; \
	fi

.PHONY: schema
schema: proto
	./libs/mage genSchema

docker-pulsar:
	docker run -d \
      -p 6650:6650 \
      -p 8080:8080 \
      --mount source=pulsardata,target=/var/pulsar/data \
      --mount source=pulsarconf,target=/var/pulsar/conf \
      apachepulsar/pulsar:2.6.1 \
      bin/pulsar standalone

docker-mongo:
	docker run -d  \
	  -p 27017:27017 \
	  --env MONGO_INITDB_ROOT_USERNAME=root \
	  --env MONGO_INITDB_ROOT_PASSWORD=root \
	  --env MONGO_INITDB_DATABASE=roava \
	  mongo:4.2.9

