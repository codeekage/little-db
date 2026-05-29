# little-db Makefile — wraps the SPEC §15 verification cookbook.
#
# Default target prints the menu so a first-time reader knows what to run.

GO ?= go
PKG ?= ./...
BENCHTIME ?= 3s

.DEFAULT_GOAL := help

.PHONY: help
help: ## Print this menu
	@echo "little-db make targets:"
	@echo "  make verify              SPEC §15 one-command verification (vet + race tests + compliance)"
	@echo "  make vet                 go vet ./..."
	@echo "  make test                go test ./... -race -count=1 -skip '^TestReq' (TestReq* is covered by 'compliance')"
	@echo "  make compliance          go test -run TestReq -v (SPEC §2 G1–G8, default workload)"
	@echo "  make compliance-heavy    same, LITTLEDB_HEAVY=1 (SPEC-scale workload + G4)"
	@echo "  make bench               go test ./internal/engine -bench=. -benchmem -benchtime=$(BENCHTIME)"
	@echo "  make build               go build ./cmd/little-db"
	@echo "  make clean               go clean -testcache"

.PHONY: vet
vet: ## Static checks
	$(GO) vet $(PKG)

.PHONY: test
test: ## Race-enabled test suite (excludes TestReq* — perf floors flake under -race; see compliance target)
	$(GO) test $(PKG) -race -count=1 -skip '^TestReq'

.PHONY: compliance
compliance: ## SPEC §2 measurable goals (default workload)
	$(GO) test ./internal/engine -run TestReq -v -count=1 -timeout 240s

.PHONY: compliance-heavy
compliance-heavy: ## SPEC §2 measurable goals (full-scale workload)
	LITTLEDB_HEAVY=1 $(GO) test ./internal/engine -run TestReq -v -count=1 -timeout 60m

.PHONY: bench
bench: ## Micro + latency benchmarks
	$(GO) test ./internal/engine -bench=. -benchmem -benchtime=$(BENCHTIME) -run='^$$'

.PHONY: build
build: ## Build the little-db CLI
	$(GO) build -o bin/little-db ./cmd/little-db

.PHONY: clean
clean: ## Drop test cache and built binaries
	$(GO) clean -testcache
	rm -rf bin

# `make verify` is the SPEC §15 contract: one command, full picture.
.PHONY: verify
verify: vet test compliance
	@echo
	@echo "verify OK: vet + race tests + SPEC §2 G1–G8 compliance all green."
	@echo "(Run 'make compliance-heavy' for the full-scale 1M-key / 5-min workload."
	@echo " Run 'make bench' for benchmark numbers.)"
