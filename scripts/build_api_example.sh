#!/usr/bin/env bash
set -euo pipefail
root="$(cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$root"
set -- target/release/build/soapnuke-rs-*/out/libsoapnuke_rapidgzip.a
if [[ $# != 1 || ! -f "$1" ]]; then
  echo 'Use a clean release build tree with exactly one SOAPnuke native archive.' >&2
  exit 2
fi
native_dir="$(dirname -- "$1")"
export TMPDIR="$root/.scratch/tmp"
mkdir -p "$TMPDIR"
exec rustc --edition=2021 -O examples/owned_api.rs \
  --extern soapnuke_api=target/release/libsoapnuke_api.rlib \
  -L dependency=target/release/deps -L native="$native_dir" \
  -L native="$native_dir/libdeflate" -o target/release/owned-api-example
