# Owned Rust API

The library crate is `soapnuke_api`; its public types and signatures are in [`job_api.rs`](../soapnuke-rs/src/job_api.rs). It uses the same native processing and job implementations as the CLI.

- `validate(JobConfig, ResourceResolver)` combines configured/supplied settings and checks typed declarations/resources. It does not consume sequencing input or create output files. Use `configuration()`, `effective_settings()`, `setting(key)` and `provenance()` to borrow the private validated authority without copying. Public fields remain compatible inspection snapshots; changing execution-relevant snapshots requires revalidation. Accessors continue to show the validated authority, and run rejects changed snapshots before input consumption.
- `run(ValidatedJob, JobIo, observer, cancellation)` consumes owned IO bindings and returns a structured result/report or a truthful partial failure. Input/output Phred is per declaration, not a single shared encoding assumption.
- `render(&NativeReport, format, sink, contract)` borrows the report and writes one artifact. Source status/populations/nulls are preserved. The caller owns and separately retains native provenance.

`JobConfig` selects Process, Demux or Dedup. `JobSpec` contains settings, layout, lanes, input/output declarations and optional physical JSON report. `Location::Path` and `Location::Handle` are distinct; supplied bindings govern IO, not dummy filenames. A fixed owned writer cannot provide a split file family. `InputHandle` distinguishes streams from genuine Read+Seek handles. Opaque owned gzip uses zlib; auto selection is reported, and explicit unsupported rapidgzip fails before consumption rather than silently falling back.

Run closes/drops owned outputs exactly once. Callers retain handles until they pass `JobIo` to run. Admission failures retain the documented zero prefix and do not create framework output/report files. Once admitted, failures preserve actual prefixes/status; if physical JSON was requested, write/flush/close errors fail the run. Cancellation and progress callbacks report legal observed prefixes rather than a scheduling guarantee.

For named exports, use the explicit pinned contract described in the [standalone guide](JOBS.md). RawR rendering emits CSV to the one sink and identifies that format/consumer contract; it is not the CLI's JSON+CSV bundle and does not contain all QC/provenance.

## Runnable authored example

After `make`, compile the included caller against the actual library:

```sh
./scripts/build_api_example.sh
mkdir api-demo
./target/release/owned-api-example merge-mixed api-demo "$PWD/examples/case-fixtures"
```

The example checks mixed-Phred owned IO, case-preserving merged bases/Q, structured counters and output close/drop. `reference-id` is another mode; it uses the supplied tiny catalogue. These are computational test fixtures, not biological truth. No consumer or formal performance qualification follows from compiling/running a new caller.
