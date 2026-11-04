# Architecture

SOAPnuke has a Rust CLI and owned Rust library backed by vendored native compression code. The five CLI commands share typed input/output declarations, decoder handling, option metadata and report structures. Process, Demux and Dedup retain their own scientific and job-specific validation rules.

| Area | Design |
|---|---|
| Configuration | A central metadata table identifies accepted option names, aliases, types, repetition and job applicability. Each job checks its cross-option rules. The library validates settings and resource declarations before input consumption. |
| Input and decoding | FASTQ declarations cover paths, streams, lanes and owned handles. Input Phred is per declaration. Opaque owned gzip uses zlib; explicit rapidgzip requires supported seekable input and worker budget. |
| Processing | Jobs carry typed failure category, stage and input ordinal. Process stages follow their declared order; Demux makes barcode assignments; Dedup groups complete sequence keys using bounded external sort runs. |
| Output | Owned writers are closed once. Reports preserve status and observed populations; failures retain actual prefixes and distinguish primary, report and cleanup errors. Dedup has a bounded fallback when detailed report serialization fails. |
| Library | `validate`, `run` and `render` expose validated jobs, owned IO and report rendering. Borrowed getters expose validated authority; changed public inspection snapshots require revalidation before execution. |
| Dependencies | Cargo dependencies and native interfaces are locked and checked. System zlib, libstdc++, pthread and C/C++ toolchains remain external. |

See the [Rust API](API.md), [build route](BUILDING.md) and [dependency contract](DEPENDENCY-INTEGRATION.md). Managed buffer limits do not bound whole-process RSS. Completion requires a successful report and checked output closure, not mere path existence.
