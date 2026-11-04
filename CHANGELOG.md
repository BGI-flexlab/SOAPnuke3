# Changes

## 3.0.0

First complete SOAPnuke 3 release. The five commands are `process`, `demux`, `dedup`, `report` and `resources`; the Rust library exposes validated owned jobs and report rendering.

- Process supports single and paired FASTQ, quality and adapter trimming, filtering, reference inspection, small-RNA processing, optional paired-end adapter-overlap clipping, correction and merging, sampling, and QC reports.
- Demux supports inline, index and declared tag barcodes. Dedup groups complete sequences and retains the earliest representative. Report renders saved QC and supports qualified named exports. Resources verifies local adapter and reference catalogues.
- The package includes locked dependencies, native compression sources, portable authored fixtures, tests, an interactive guide and offline ZIP.
- JSON report sections use command/function names (`processing`, `demux`, `dedup`) and the build identity is `soapnuke-3.0.0`.

See [documentation](README.md#documentation-and-validation) for function behavior, limitations and validation scope.
