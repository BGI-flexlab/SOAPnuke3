# Standalone jobs

Use `SOAPnuke COMMAND --help` for every option and default. Export is a mode of `report`, not a sixth top-level command. The authored examples in `tests/fixtures` are small computational controls, not experimental reads.

## Demux

A sample table has tab-separated `sample`, `barcode1`, `barcode2` columns. Inline mode captures declared coordinates; index mode consumes synchronized I1/I2 records; tag mode uses the declared header label. Read/mate/index names and counts must associate. Equal best barcode distances are ambiguous; no match, excessive N or low label quality is unassigned. Outputs retain source order. Index mode does not remove sequencing bases and rejects an explicitly supplied `barcode-remove`, including false. `--i2` is required exactly when the table supplies a nonempty second barcode.

```sh
mkdir demo-demux
SOAPnuke demux --r1 tests/fixtures/demux-inline-se/input.R1.fastq \
  --samples tests/fixtures/demux-inline-se/samples.tsv \
  --barcode-source inline --barcode-r1 1:4 --barcode-errors 1 \
  --barcode-remove=true --out-dir demo-demux/samples \
  --output-template '{sample}.R{mate}.fastq' --report demo-demux/report.json \
  --ambiguous1 demo-demux/ambiguous.fq --unassigned1 demo-demux/unassigned.fq --threads 2
```

This routes two of four units; one is ambiguous and one unassigned. Required sample destinations can be empty. Unused optional auxiliary files remain absent. PE, interleaved, multiple-lane and prefix modes keep their declared associations.

## Dedup

Keys are the complete uppercase SE sequence or ordered PE sequence tuple. Headers, qualities, UMI and coordinates are not key fields. Full keys are compared; the earliest global input ordinal is representative. Representative and duplicate streams each preserve original order, case, headers and numeric Q. Prefix selection, filtering, transformations and sampling are not part of this job.

```sh
mkdir demo-dedup
SOAPnuke dedup --r1 tests/fixtures/dedup-se/input.R1.fastq \
  --out1 demo-dedup/representatives.fq --duplicates1 demo-dedup/duplicates.fq \
  --spill-dir demo-dedup/spill --report demo-dedup/report.json --threads 2
```

There are six authored reads and three keys. Grouping uses bounded external storage; scratch space and file limits matter. Product-owned internal spills are cleaned; declared partial outputs remain truthful on failure. This is exact sequence equality, not molecular duplicate identification.

## Saved reports and exports

`report` reads native JSON without recomputing QC. Saved status, input/accepted/written populations and unavailable/null values are preserved. Job-specific report fields differ; the shared family/version does not imply one uniform Process schema for every job.

```sh
SOAPnuke report --input-report demo/SOAPnuke.json \
  --html-report demo/qc.html --tsv demo/qc.tsv
```

Use fresh output names with existing parents. Named exports need explicit metadata, a pinned consumer version and a producer/artifact-bound qualified contract. Old-producer contracts are rejected; `--allow-unqualified` produces an explicitly unqualified bootstrap artifact, not consumer acceptance.

The MultiQC surface is custom content for 1.27.1, tested with `--fullnames`. The MGnify surface targets RawR commit `f781cef13b5448eaf3673a5d82550d0267086480` input-validator CSV compatibility only, not QC ingestion or full workflow acceptance. Its explicitly supplied workflow sample IDs use `[A-Za-z0-9_.-]+`; original native labels remain in provenance. CLI RawR export is JSON plus CSV. Library render has one sink and emits CSV only, while the caller retains native provenance separately. The formal build has producer-specific pinned-consumer checks described in [validation](VALIDATION.md). A new producer or linked caller needs its own qualification.

## Limits and failures

Charged statistics are bounded at 128MiB and pipeline/features at 512MiB, optionally with a joint `--memory-mib` limit. These are not whole-RSS bounds; caller storage, allocator overhead, stacks, opaque caches and declared exclusions remain distinct. Report/contract size limits are listed in module help. Detailed Dedup report serialization can fail with exact scalar summaries and explicitly unavailable detailed QC; primary, report and cleanup failures remain distinct. Output-set completion requires valid successful JSON, not path existence. No portable deployment, general multi-file transaction or browser-appearance guarantee is claimed.
