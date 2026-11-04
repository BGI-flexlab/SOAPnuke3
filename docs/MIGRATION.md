# Familiar SOAPnuke parameters on one v3 processing command

Use `SOAPnuke process` for all supported processing. Familiar v1/v2 parameter spellings and the existing v3 names enter the same v3 processing path. The separate legacy `filter` command has been removed. Calling `filter` now exits with a migration message before processing files; it does not silently select different behavior.

The `process` command retains **v3 filtering rules and defaults**. Familiar spellings provide familiar syntax, not a promise of identical v1/v2 results. Review the differences below when adapting an existing script.

## Common parameters

| Familiar spelling | Existing v3 spelling / operation | Notes |
|---|---|---|
| `-1`, `--fq1` | `--r1` | Single-end input or mate 1. |
| `-2`, `--fq2` | `--r2` | Mate 2. |
| `-C`, `--cleanFq1` | `--out1` | Clean single-end output or mate 1. |
| `-D`, `--cleanFq2` | `--out2` | Clean mate 2. |
| `-o`, `--outDir` | `--out-dir` | Existing directory for relative output paths; inputs/resources remain relative to the working directory. |
| `-T`, `--thread` | `--threads` | Same v3 CPU-budget validation. |
| `-l`, `--lowQual` | `--low-q` | Counts bases with quality strictly below the threshold; supply with `-q`. |
| `-q`, `--qualRate` | `--max-low-q-fraction` | Rejects when the fraction is strictly above the limit; supply with `-l`. |
| `-n`, `--nRate` | `--max-n-fraction` | Rejects when the N fraction is strictly above the limit. |
| `-m`, `--mean` | `--min-mean-q` | Minimum mean quality. |
| `-4`, `--minReadLen`, `--minLen` | `--min-length` | Minimum read length. |
| `--maxReadLen` | `--max-length` | Maximum read length. |
| `-f`, `--adapter1` | `--adapter3-r1` | Repeatable SE/mate-1 3′ adapter sequence. |
| `-r`, `--adapter2` | `--adapter3-r2` | Repeatable mate-2 3′ adapter sequence, under the v3 matching contract. |
| `-J`, `--ada_trim`, `-E`, `--cutAdaptor` | `--adapter-action trim` | No argument; trimming is already the v3 default. |
| `-t`, `--trim` | `--cut-r1`, optionally `--cut-r2` | Two values mean R1/SE head,tail; four mean R1 head,tail,R2 head,tail. |
| `-Q`, `--qualSys` | `--input-phred` | Translate `1` to `64`, `2` to `33`. |
| `--outQualSys` | `--output-phred` | Translate `1` to `64`, `2` to `33`. |
| `-G` | `--output-phred 33` | No argument. |
| `-w`, `--cleanOutSplit`, `--output_clean` | `--split-units` | Requires `{chunk}` in main output paths; a unit is one SE read or PE pair. |
| `-h`, `--help`; `-v`, `--version` | Same help/version operations | At the top level or immediately after `process`. |

Short values can be separate (`-T 16`), attached (`-T16`) or use `=` (`-T=16`). Long options accept either `--thread 16` or `--thread=16`. Do not specify two spellings of the same scalar option: `-T 16 --threads 32` is an error. Repeatable adapter options may mix spellings and preserve their order.

## Output directory and reports

```sh
mkdir -p results
SOAPnuke process -1 reads_R1.fq.gz -2 reads_R2.fq.gz \
  -C clean_R1.fq.gz -D clean_R2.fq.gz -o results -T 16
```

This writes `results/clean_R1.fq.gz`, `results/clean_R2.fq.gz` and `results/SOAPnuke.json`. The default JSON filename is recorded as a default in report provenance, not as an explicitly supplied option.

Use `--report sample.qc.json` to choose another report name within `-o`. Relative FASTQ, JSON, HTML and match-audit outputs all use that directory. Absolute output paths remain absolute, and `-` still denotes stdout where supported. Do not prefix a relative output with the directory again: with `-o results`, use `-C clean.fq.gz`, not `-C results/clean.fq.gz`.

The directory must already exist. Existing output files are not overwritten, and the existing v3 filesystem/publication checks still apply. Without `-o`, output paths retain the original v3 working-directory behavior and `--report` remains required. JSON/optional HTML reports are produced; historical v1/v2 text-report formats are not restored.

## Behavior differences to review

| Topic | This unified v3 command |
|---|---|
| Quality and N filters | Off unless enabled. `-l` and `-q` must be supplied together. No implicit v2 `-l 5 -q 0.5 -n 0.05` profile is installed. |
| Threshold equality | Low-quality base means `Q < -l`. Reject when low-quality fraction `> -q` or N fraction `> -n`. The retained v2 implementation used inclusive comparisons at these boundaries. |
| Minimum read length | Default remains 1, rather than the v2 ordinary-filter default of 30. Set `-4 30` when that is intended. |
| Quality encoding | Input and output default to Phred+33. Do not rely on the v1.6.5 Phred+64 input default. Use `-Q 1` for Phred+64 input. |
| Adapters | Default action remains trim. Select `--adapter-action discard` explicitly to discard matches. V3 matching rules are unchanged and are not claimed to reproduce every historical alignment decision. |
| Adapter files | `-f/-r` accept sequences. For files use `--adapter-file` with named v3 FASTA headers such as `>adapter1;mate=R1;end=3`. Historical one-sequence-per-line lists are not automatically interpreted. |
| Disabled controls | Omit an optional filter to disable it; old negative sentinel values are not adopted. |
| Thread default | Remains the v3 default: 8, or the detected/scheduler CPU ceiling if smaller. |
| Splitting | Uses explicit `{chunk}` templates and v3 source-unit counting. Old automatic split-filename conventions are not restored. |

For example, to explicitly request quality and length filtering:

```sh
mkdir -p results
SOAPnuke process -1 reads_R1.fq.gz -2 reads_R2.fq.gz \
  -C clean_R1.fq.gz -D clean_R2.fq.gz -o results -T 16 \
  -l 20 -q 0.2 -n 0.05 -4 30
```

## Configuration and unsupported historical options

`-c` meant retained-read quantity in v1 and configuration-file input in v2. It is rejected, together with `--configFile`, with directions to use `--config` for typed v3 JSON. Historical `key=value` configuration files are not supported. `-L` also had conflicting historical meanings and is rejected with directions to explicit v3 controls; exact-count legacy sampling is not implied by fraction or input-prefix controls.

JSON retains canonical v3 names, without dashes, for example:

```json
{
  "job": "process",
  "options": {
    "r1": "reads.fq.gz",
    "out1": "clean.fq.gz",
    "out-dir": "results",
    "threads": 16
  }
}
```

CLI aliases override corresponding canonical config keys. Duplicate aliases are detected, and invalid configured values cannot be hidden by CLI overrides. Report provenance uses canonical option names.

Other unsupported historical controls and specialized modules remain unsupported. They produce errors rather than choosing a legacy execution engine. In particular, v1's numeric `-J` is not the v2/v3 no-argument trimming switch; do not carry it over without reviewing its purpose.
