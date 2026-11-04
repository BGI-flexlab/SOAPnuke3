# Build and portable tests

Build on Linux with the tested Rust/Cargo 1.99.1, C/C++17, CMake, Make, zlib headers and Python3.10+. Toolchain/system libraries are external; minimum Rust version and byte-identical/portable binaries are not promised. Rust dependencies and native compression sources are vendored and locked. Large real-data and timing inputs are not needed or included.

```sh
make JOBS=2
make test JOBS=2 TEST_THREADS=2
./scripts/build_api_example.sh
```

`make test` runs the complete native unit suite, the explicitly ignored 120-second parser/transform mutation test, both portable CLI runners, and two actual owned-library examples. `make test-all` is a compatibility alias for that complete route; `make test-slow` runs the deliberately slow tests separately. Scientific assertions are retained. Native tests use repository-owned literals and fresh per-invocation workspaces under `.scratch/tmp/native`, including the previously external header/catalogue/storage controls. No external fixture environment is required.

`tests/fixtures/native/PROVENANCE.json` records the authored computational controls. Three grouping cases share one 343-unit payload; storage limits distinguish the cases. These fixtures are not experimental sequencing evidence.

`make legacy-cli-test` and `make cli-test` remain individually runnable. CLI outputs use fresh repository-local directories. The linked API examples cover mixed-Phred merging and a reference resource ID with actual IO/counters/close/drop. Use a clean build tree so the native archive is unambiguous. Actual external named-consumer qualification and large-data/performance studies remain separate integrations with explicit dependencies; they are not silently represented as unit-test passes.

`scripts/build_identity.py` records compile/dependency source hashes before build. The source manifest records the current compile inputs; a new build's executable/caller hashes and runtime context are separate. For an API caller linked manually, the native link directories must be the actual Cargo build outputs. See the example build script rather than linking an unrelated installed library.

Run `make dependency-check` for locked/vendor/patch/native-ABI identity checks, or `make dependency-integration` for those checks plus the complete native/slow/CLI/actual-library route. See [the dependency upgrade contract](DEPENDENCY-INTEGRATION.md).

## Optional pinned fastp comparison

After building, the following independent integration checks the changed F07/P03/P04 rules against an explicitly supplied **fastp 1.4.0** executable. It generates small authored inputs and retains commands, outputs and comparisons in a new directory; it does not download a tool or dataset.

```sh
python3 tests/compare_fastp.py --soap target/release/SOAPnuke \
  --fastp /path/to/fastp-1.4.0 --out .scratch/tmp/fastp-comparison-new
```

This is separate from `make test` because fastp is an external test dependency. It checks exact decoded sequences and qualities for unique overlaps; it does not assert whole-pipeline or overlap-selector equivalence.
