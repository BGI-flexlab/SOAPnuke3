# Local resources and optional catalogue IDs

Resources are adapter/reference FASTA files, not sequencing reads. A catalogue assigns names to files you already have. Nothing is downloaded, and no catalogue is selected implicitly. Direct files still work without one:

```sh
SOAPnuke process --r1 reads.fq.gz --out1 NEW_clean.fq.gz --report NEW_report.json \
  --adapter-file adapters.fa
```

A catalogue has `schema: "soapnuke-v3.resources/1"` and a `resources` array. Each row requires unique `id`, `kind` (`adapter` or `reference`), `path`, and lowercase `sha256`. Relative paths resolve beside the catalogue; optional version/description remain unavailable when absent. SHA256 refers to exact original file bytes, not normalized sequences.

```sh
SOAPnuke resources list --resource-catalog examples/case-fixtures/catalogue-expanded.json
SOAPnuke resources show adapter-lower --resource-catalog examples/case-fixtures/catalogue-expanded.json
SOAPnuke resources verify adapter-lower --resource-catalog examples/case-fixtures/catalogue-expanded.json
```

`list`/`show` display declarations. `verify` also checks the file, kind, digest and FASTA format. The included catalogue is authored test data, not a default database; N-bearing adapter entries deliberately exercise rejection.

```sh
SOAPnuke process --r1 reads.fq.gz --out1 NEW_clean.fq.gz --report NEW_report.json \
  --resource-catalog examples/case-fixtures/catalogue-expanded.json \
  --adapter-file id:adapter-lower
```

Process `--adapter-file` and `--screen-reference` accept direct files or `id:ID`; sequence-valued adapter flags do not accept IDs. Prefix a literal path beginning `id:` with `./`. Unknown IDs, wrong kind/schema, missing files and mismatched digests fail. Adapter headers use `>name;mate=R1;end=3` (R1/R2 and3/5 supported). Adapter sequences accept A/C/G/T in either case and reject N/n/other ambiguity. Reference FASTA accepts IUPAC, but only canonical windows match; case does not change matching, and N/IUPAC never act as wildcards.
