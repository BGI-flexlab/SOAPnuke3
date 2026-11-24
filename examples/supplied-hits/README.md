# Remove pairs using an external hit table

These two artificial pairs have hand-labelled screening results. Only R2 of the first pair (`flagged`) has a hit. The second pair (`clear`) has none.

From the repository root, with `SOAPnuke` on your path, run:

```sh
sh examples/supplied-hits/run.sh sample-screen-output
```

Use a new output directory each time. The [script](run.sh) runs `process` with `--screen-action remove --screen-pair any`, so either mate can exclude the pair. It compares both clean FASTQs to the included [expected R1](expected.R1.fq) and [expected R2](expected.R2.fq). Each contains only `clear`. The report counts two input pairs, one accepted and one rejected. With `--screen-pair both`, both mates would need hits to exclude a pair, and both example pairs would pass.

## Prepare these files for your own reads

1. Write [the TSV](hits.tsv) with the tab-separated header `source_ordinal`, `mate`, `hit`. Number the original input units from zero. Each single-end read has one row (mate 1). Each pair has two rows (mates 1 and 2) with the same ordinal. Supply exactly one lowercase `true` or `false` for **every** original mate, including units beyond a processing limit. Read names are not columns.
2. Save [the companion JSON](hits.tsv.provenance.json) beside the TSV, adding `.provenance.json` to its complete filename. Keep the illustrated version, population, ordinal origin and aggregation strings. Set `layout` to `SE` or `PE`, `input_phred` to the input encoding, and `source_units` to the full number of reads (SE) or pairs (PE).
3. List the original files in input order under `inputs`. Each entry has a mate, a path label, and the SHA-256 of the **exact file bytes** (the compressed bytes for gzip). Obtain the digests with `sha256sum` on the files passed to SOAPnuke. For interleaved PE use one entry with `"mate": "interleaved"` and set top-level `"interleaved": true`. For a lane manifest, list files in manifest order and include each entry’s `lane` ID.
4. Record the decision provider's name, version and argument list. List the references it used with an identity (for example `id`) and a SHA-256 for each. The empty reference list in this example describes hand-labelled decisions. SOAPnuke reads the decisions from the TSV and checks their binding to the original FASTQ inputs.
