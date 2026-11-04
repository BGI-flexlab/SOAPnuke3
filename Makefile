SHELL := /bin/bash
JOBS ?= 2
TEST_THREADS ?= 2

.PHONY: all test test-all test-slow cli-test legacy-cli-test
all:
	./scripts/cargo.sh build --release --locked --offline --jobs $(JOBS)

test: all
	./scripts/cargo.sh test --release --locked --offline --jobs $(JOBS) --lib -- --test-threads=1
	$(MAKE) test-slow cli-test legacy-cli-test adapter-overlap-test
	./scripts/build_api_example.sh
	@mkdir -p .scratch/tmp
	./target/release/owned-api-example merge-mixed "$$(mktemp -d "$(CURDIR)/.scratch/tmp/api.XXXXXX")" "$(CURDIR)/examples/case-fixtures"
	./target/release/owned-api-example reference-id "$$(mktemp -d "$(CURDIR)/.scratch/tmp/api.XXXXXX")" "$(CURDIR)/examples/case-fixtures"

test-all: test


test-slow:
	./scripts/cargo.sh test --release --locked --offline --jobs $(JOBS) -- --ignored --test-threads=1

legacy-cli-test:
	@mkdir -p .scratch/tmp
	PYTHONDONTWRITEBYTECODE=1 TMPDIR="$(CURDIR)/.scratch/tmp" python3 tests/check_cli.py --binary target/release/SOAPnuke --threads $(TEST_THREADS) --out "$$(mktemp -d "$(CURDIR)/.scratch/tmp/cli.XXXXXX")/results"

cli-test:
	@mkdir -p .scratch/tmp
	PYTHONDONTWRITEBYTECODE=1 TMPDIR="$(CURDIR)/.scratch/tmp" python3 tests/check_modules.py --binary target/release/SOAPnuke --threads $(TEST_THREADS) --out "$$(mktemp -d "$(CURDIR)/.scratch/tmp/modules.XXXXXX")/results"

.PHONY: dependency-check dependency-integration adapter-overlap-test
adapter-overlap-test:
	@mkdir -p .scratch/tmp
	PYTHONDONTWRITEBYTECODE=1 TMPDIR="$(CURDIR)/.scratch/tmp" python3 tests/check_adapter_overlap.py --binary target/release/SOAPnuke --out "$$(mktemp -d "$(CURDIR)/.scratch/tmp/adapter-overlap.XXXXXX")/results"

# The complete maintained route includes the named parser/native/API checks.
dependency-check:
	python3 scripts/check_dependencies.py

dependency-integration: dependency-check test
