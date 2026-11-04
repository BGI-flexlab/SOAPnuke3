# Locked dependency and native integration contract

Use `make dependency-check` before rebuilding or refreshing vendored sources.
It checks the current Cargo lock, all vendored file checksums, unchanged bundled
native sources, the native interface/wrapper sources, and the retained private
serde_json hook. `make dependency-integration` adds the complete maintained test
route, including the explicit slow test and actual linked owned-IO example.
No download, dependency refresh or system-library installation is performed.

The Rust registry identities are fixed by Cargo.lock. The local serde_json
1.0.154 fork retains the upstream registry checksum and VCS metadata, with its
patched per-file checksum recorded in the vendor checksum file. Its only local
hook reserves scratch before borrowed RawValue slice parsing under raw_value,
checks actual capacity before parsing, and leaves ordinary serde parsing alone.
The exact retained patch/integration hashes are in
`tests/dependency-contract.json`; they are not an upstream-byte equivalence claim.
Native sources retain libdeflate 1.26 and rapidgzip 0.16.0 with the recorded
librapidarchive submodule commits. Native ABI declarations and wrappers are
hash-bound; the system zlib/libstdc++/pthread and C/C++ toolchains remain external.
Supported evidence is Linux x86-64 with Rust/Cargo 1.99.1 and C++17, not universal
portability or byte-identical rebuilding.

When intentionally upgrading, preserve the previous lock/vendor/patch/native
identity, review the exact diff and the C ABI signatures/ownership rules, then
update the contract only after testing. Compilation alone does not establish
scratch/codec/lifetime compatibility. Required named checks in the full route:

- `report_app::tests::report_borrowed_capacity_nested_escaped_limits_release`
  checks nested/escaped raw JSON, simultaneous-document/scalar limits, managed
  capacity and release; ordinary Process uses its existing serde path.
- `rapidgzip_reader::refill_tests::*` and `rapidgzip_reader::tests::*` check
  partial refill/consumption, CRC/member/truncation, cancellation and CPU permits.
- `zlib_reader::tests::*` checks concatenated gzip and wrapper restrictions.
- `source::owned_reader_tests::*` checks framing/decoder failures and owner drop.
- Dedup storage/finalization tests check real-file simulated write/flush/close
  failures and truthful cleanup/report outcomes; they do not claim OS quota tests.
- `scripts/build_api_example.sh` links the actual Rust/native archives; its
  mixed-Phred and resource-ID modes check literal records/QC and close/drop.

Managed reservations, caller-owned storage and process RSS are separate facts.
A dependency update must retain all relevant source-based capacity and lifetime
arguments as well as executable assertions. Consumer or timing receipts from another build
cannot qualify a newly built producer.
