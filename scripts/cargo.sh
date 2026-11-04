#!/usr/bin/env bash
set -euo pipefail
root="$(cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$root"
export TMPDIR="$root/.scratch/tmp"
export CARGO_HOME="$root/.scratch/cargo-home"
export CARGO_TARGET_DIR="$root/target"
mkdir -p "$TMPDIR" "$CARGO_HOME" "$root/.scratch/build"
python3 scripts/build_identity.py
export SOAPNUKE_BUILD_ID=soapnuke-3.0.0
export SOAPNUKE_SOURCE_SHA256="$(sha256sum .scratch/build/source-manifest.json | cut -d ' ' -f 1)"
export SOAPNUKE_DEPENDENCY_SHA256="$(sha256sum .scratch/build/dependency-manifest.json | cut -d ' ' -f 1)"
command="$1"
shift
exec cargo "$command" --manifest-path soapnuke-rs/Cargo.toml "$@"
