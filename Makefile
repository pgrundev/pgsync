# Developer entry points. Requires Docker and the Rust stable toolchain.

SOURCE_URL ?= postgres://pgsync:pgsync@localhost:5433/app
TARGET_URL ?= postgres://postgres:postgres@localhost:5434/app
E2E_DIR    ?= target/e2e

.PHONY: build test lint fmt up down e2e e2e-slow live check bootstrap run status cleanup bench

build:
	cargo build --release

test:
	cargo test --workspace

lint:
	cargo fmt --all -- --check
	cargo clippy --workspace --all-targets -- -D warnings

fmt:
	cargo fmt --all

up:
	docker compose up -d --wait

down:
	docker compose down -v

## Live protocol test + fast end-to-end scenarios (about 6 minutes).
e2e: up
	PGSYNC_TEST_SOURCE_URL=$(SOURCE_URL) cargo test -p pgsync-postgres --test live_replication
	PGSYNC_E2E=1 PGSYNC_E2E_DIR=$(E2E_DIR) cargo test -p pgsync --test e2e -- --test-threads=1

## Disruptive scenarios: container pause/restart, random crash loop, large table.
e2e-slow: up
	PGSYNC_E2E=1 PGSYNC_E2E_DIR=$(E2E_DIR) cargo test -p pgsync --test e2e -- --test-threads=1 --ignored

check bootstrap run status:
	PGSYNC_SOURCE_URL=$(SOURCE_URL) PGSYNC_TARGET_URL=$(TARGET_URL) cargo run -q --release -- --config examples/pgsync.yaml $@

cleanup:
	PGSYNC_SOURCE_URL=$(SOURCE_URL) PGSYNC_TARGET_URL=$(TARGET_URL) cargo run -q --release -- --config examples/pgsync.yaml cleanup --yes

bench: build
	SOURCE_URL=$(SOURCE_URL) TARGET_URL=$(TARGET_URL) ./scripts/bench.sh
