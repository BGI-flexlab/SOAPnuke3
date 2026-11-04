# Validation scope

The maintained source test route is `make test JOBS=2 TEST_THREADS=2`. It includes native tests, portable Process and standalone-command CLI suites, the explicit long mutation test and two linked owned-library examples. Authored fixtures are bundled; see [building](BUILDING.md). The [machine-readable release validation summary](validation-summary.json) records the formal build identity, checks and limits. The optional pinned fastp differential check and dependency integration route are documented there.

## Formal 3.0.0 Linux x86-64 build checks

The formal executable reports build ID `soapnuke-3.0.0` and has SHA256 `17f371826e6b7435a8c017bc95f7762838486f21892e0303451e4b606e0831c8`. Its source manifest SHA256 is `709cb22cbfbb53d057154097e0d649ef0f06b0dc25c78762b0f4dd7af158e813`. These identities distinguish the release build from inherited measurements.

| Check on this executable | Observed result |
|---|---|
| Native tests | 318 normal tests passed; the separately run 120-second mutation test passed. |
| CLI and dependency route | 49 Process CLI cases, 24 standalone/help calls, focused adapter-overlap checks and dependency checks passed. |
| Owned Rust API | 30 linked caller definitions and both freshly rebuilt owned-library examples passed. |
| Named exports | 18 CLI and 18 API profiles, 14 actual pinned-consumer calls (eight MultiQC and six RawR), four fresh receipts/contracts, and seven qualified CLI plus seven qualified API follow-ups passed. Scope is custom-content/parser and input-table compatibility only. |
| Real-read parity | Twelve fresh calls with adapter overlap on/off across six fixed subsets (600,000 distinct pairs) passed exact decoded FASTQ sequence/quality, full numeric QC, disposition and adapter-overlap counter comparisons against frozen controls. |
| Resource smoke | Eight calls (one formal/baseline pair each for SE and PE at 16 and 32 threads) passed exact output hashes, numeric QC, paired peak RSS ≤ baseline + max(10%, 16 MiB), statistics ≤128 MiB, tracked chunks and prefetch ≤512 MiB, absolute RSS ≤2 GiB, 600-second timeout and at least five seconds per call. |
| Interactive guide | Local Chromium checked 25 operation panels across five commands, adapter-overlap SE/PE off/on states and mobile layout; no JavaScript errors or network requests. Offline ZIP contains the same HTML bytes. |

This resource check is one paired comparison per cell, not a timing campaign or a general performance estimate. The maintained tests and bounded real-read calls do not establish full-WGS, all-parameter, scaling, or downstream biological qualification.

## Linux ARM64 and Conda packages

The same 3.0.0 source passed the full maintained `make test` route on a [native Ubuntu 24.04 ARM64 runner](https://github.com/BGI-flexlab/SOAPnuke3/actions/runs/41795521134): 318 normal native tests, the separate 120-second test, portable CLI and adapter-overlap checks, and both linked owned-library examples. The standalone ARM executable has SHA256 `ed3ed5892c49fd42b576d1e43ca5bd1efbc5e362f618b3d14adb3db28e26e5f0`.

The downloadable [Linux x86-64 and ARM64 Conda packages](https://github.com/BGI-flexlab/SOAPnuke3/actions/runs/41802552250) each passed 318 native tests, portable CLI and adapter-overlap checks, then installation into a fresh Conda test environment with help/version and an actual filtering assertion. The package tests do not include the separately run 120-second test. A scheduling-dependent contention test was fixed by making its workers queue before releasing the sole permit; all original assertions remain. This patch changes only test code. Source algorithms and bundled libraries are unchanged; the native toolchain and runtime-library packaging differ. See [installation and compatibility requirements](BUILDING.md). These platform checks do not extend the x86-64 release's real-data or performance measurements to ARM.

## Additional inherited study

The 3.0.0 source incorporates the separately tested adapter-overlap implementation. Its pre-formal candidate passed 319 native tests (including the 120-second smoke), 49 Process CLI calls, 24 module/help calls, both owned-library examples, dependency checks and focused adapter-overlap fixtures. These are **inherited candidate results**, not measurements of a newly built 3.0.0 executable.

The measured pre-formal executable had SHA256 `697c0c78443e5fb542b1b2b25eb8e8e2896dd99429b193c57d56cd3c28cf484d`. The verified archive receipt is `archive/development-previews-20261104/archive-receipt.json` in the project workspace. The binary and source are stored in that archive directory. Full commands, manifests and outputs remain in the project evidence workspace; none are distributed in this repository.

Across six fixed subsets totaling 600,000 distinct paired reads, the candidate passed independent output sequence/quality, every-position QC, disabled-option parity and exhaustive adapter-overlap audit checks. There were no overlap-clipping decisions in 169,356 reference-derived WGS long-insert negative controls, including 85 reference-contradictory pairs identified in the initial comparison. These controls are consistency proxies, not experimentally labeled truth. Additional clips beyond ordinary supplied-adapter matching were:

| Library | First 100,000 pairs | Next 100,000 pairs |
|---|---:|---:|
| WGS SRR37356337 | 19 | 27 |
| Ribodepleted RNA SRR24511772 | 1,319 | 1,313 |
| Oligo-dT RNA ERR15897279 | 200 | 216 |

These counts require both full-overlap and supplied-adapter support. They do not prove downstream improvement. The report's `clipped_pairs` also includes decisions ordinary matching would already make. The assessed input prefixes are not whole-library prevalence estimates.

All 40 ordinary-processing resource calls passed the existing output/QC and memory gates. In 16 separate repeated RNA timing calls with adapter overlap enabled, median wall cost was +7.54% at 16 threads and +2.82% at 32 threads; CPU cost was +21.84% and +19.32%, relative to ordinary adapter matching. These are shared-node, bounded-workload observations, not full-WGS or scaling estimates.

Earlier source validation covered exact decoded bases and numeric qualities for low-quality/complexity boundaries and overlap correction/merge against pinned fastp 1.4.0, plus named-export parser/input-table compatibility against pinned MultiQC 1.27.1 and RawR commit `f781cef13b5448eaf3673a5d82550d0267086480`. Those earlier consumer checks were bound to their tested producer and caller. The formal 3.0.0 producer-specific qualification is summarized above; future builds and newly linked callers need their own evidence.
