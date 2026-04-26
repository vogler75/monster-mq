SHELL := /bin/bash
BIN := bin/monstermq-edge
PKG := ./cmd/monstermq-edge

LDFLAGS := -s -w
GOFLAGS := -trimpath

.PHONY: build build-arm64 build-armv7 test test-race lint clean gen run

build:
	@mkdir -p bin
	CGO_ENABLED=0 go build $(GOFLAGS) -ldflags="$(LDFLAGS)" -o $(BIN) $(PKG)

build-arm64:
	@mkdir -p bin
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build $(GOFLAGS) -ldflags="$(LDFLAGS)" -o bin/monstermq-edge-linux-arm64 $(PKG)

build-armv7:
	@mkdir -p bin
	GOOS=linux GOARCH=arm GOARM=7 CGO_ENABLED=0 go build $(GOFLAGS) -ldflags="$(LDFLAGS)" -o bin/monstermq-edge-linux-armv7 $(PKG)

build-amd64:
	@mkdir -p bin
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build $(GOFLAGS) -ldflags="$(LDFLAGS)" -o bin/monstermq-edge-linux-amd64 $(PKG)

build-all: build-amd64 build-arm64 build-armv7

test:
	go test ./... -count=1 -timeout 60s

test-race:
	go test ./... -race -count=1 -timeout 120s

lint:
	go vet ./...

clean:
	rm -rf bin

gen:
	go run github.com/99designs/gqlgen generate

run: build
	$(BIN) -config config.yaml.example
