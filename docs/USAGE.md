# Feature guide

Use `SOAPnuke process --help` or the [complete option reference](process-help.txt)
for syntax, allowed combinations and defaults. The operations below all belong
to `process`; they are not separate subcommands.

## Adapter matching

Adapters are library-preparation sequences that can appear in a read when
sequencing extends beyond the insert. Supplying the correct adapter allows
SOAPnuke to remove that sequence before length/quality filtering.

For a 3′ adapter, SOAPnuke tries its possible starting positions in the read.
At each position it compares the overlapping bases without gaps. A match must
reach `--adapter-min-overlap` and have a mismatch fraction no greater than
`--adapter-max-error`. Among qualifying matches, selection favors more compared
bases, then fewer mismatches, then an earlier start, then declaration order.
The sequence from the selected adapter start to the end of the read is removed.

For example, an exact match of `AGATCGGAAGAGC` in
`ACGTACGTAGATCGGAAGAGC` leaves `ACGTACGT`. With the default minimum overlap of
3, a partial adapter at the read end may also match. Short matches can occur by
chance; choose the overlap/error limits for the library and intended analysis.

`--adapter5-r1` and `--adapter5-r2` match at the start of the read and remove
the matched prefix. Adapter sequences contain A/C/G/T only. This matcher
allows substitutions; it does not implement gapped alignment, indel handling
or general IUPAC wildcards. It is not a claim of Cutadapt-equivalent matching.

Use `--adapter-file adapters.fa` for multiple named adapters, for example:

```text
>adapter1;mate=R1;end=3
AGATCGGAAGAGC
>adapter2;mate=R2;end=3
AGATCGGAAGAGC
```

`--infer-adapter r1`, `r2` or `both` enables prefix-based 3′ adapter inference.
Its prefix size and support threshold are controlled by `--infer-units` and
`--infer-min-support`. Inference is an estimate, not a guarantee that every
library's adapter will be identified. Prefer known sequences when available.
`--match-report` can retain a bounded audit of matches.

## Paired-end adapter-overlap clipping

`--adapter-overlap` is an optional, default-off adapter-stage operation for ordinary paired-end processing. It requires trim action and explicit supplied 3′ adapters for both mates, through `--adapter3-r1`/`--adapter3-r2` or named R1/R2 records in `--adapter-file`. Inference alone and adapter discard cannot be combined with it.

For each pair, the operation seeks a unique insert boundary shorter than both reads. It compares the full inferred insert in R1 with the reverse complement of R2 using canonical bases. At least 30 bases must be compared, with at most `min(3, floor(0.02 × compared bases))` mismatches. Both exposed tails must consist of A/C/G/T and match a prefix of their respective supplied adapter. One- or two-base tails match exactly; longer tails allow at most `floor(tail length / 10)` mismatches. If multiple boundaries pass, only one tail matches, or ordinary sequence matching would cut either mate earlier, overlap clipping abstains. Ordinary adapter matching still runs.

Both sequence and quality strings are clipped at the same boundary. The operation is unavailable after an earlier change to the read's 5′ start: positive fixed head cuts, capture removal or retention, 5′ adapters, or quality Ends/Mott before Adapters. Terminal-run trimming and Window quality trimming remain supported; Ends/Mott after Adapters are supported. Its fixed thresholds are separate from ordinary adapter match and later PE correction/merge overlap settings.

When enabled, `processing.adapter_overlap` in the JSON report records evaluated, clipped, ambiguous and abstained pairs. `--match-report` can record accepted decisions, subject to `--match-limit`; its `overlap:` adapter label identifies a compatible supplied motif, not the physical adapter with certainty. Counts represent algorithm decisions. See [validation](VALIDATION.md) for the bounded real-read assessment.

## Trimming and filtering

Fixed cuts remove a declared number of bases. For paired data,
`-t 5,0,5,0` removes five bases from the start of each mate. Within ordinary
processing, fixed cuts precede the tail/quality/adapter stages. The default
order of those three stages is `tails,quality,adapters`; `--stage-order` can
change it. Read filtering is applied to the processed sequence.

`--quality-mode ends` trims low-quality ends; `window` uses local quality
windows; `mott` uses a quality-score-based clipping calculation. Each enabled
mode requires `--quality-q`. A window size is specified with
`--quality-window`. These modes may retain different sequences, even when
given the same numeric quality threshold.

`--tail-bases G --tail-min 10` enables terminal poly-G trimming. Other allowed
bases or `all` can be selected explicitly. This operation is off by default.

Read filters can reject short/long reads, excess Ns, excess low-quality bases,
low mean quality, or low sequence complexity. Complexity is measured using
the fraction of adjacent characters that change, including transitions to/from N,
as in fastp 1.4.0; it is not an entropy estimator. With this filter enabled,
length <=1 fails; equality with the cutoff passes. SOAPnuke uses a fraction
(0.3), whereas fastp uses a percentage (30).
Use `--reject1/--reject2` if rejected records are needed for inspection.

## Paired-end decisions, correction and merging

The default `--pair-policy both-pass` requires both mates to pass ordinary
read filters. `either-pass` or `r1-pass` changes that rule. Explicit
reference-screening or insert-length rejection still rejects the pair.

`--singleton1` and `--singleton2` retain passing mates whose partner failed;
request both paths and use `both-pass`. These additional outputs do not
receive the main output's sampling or splitting.

Overlap handling compares the mates in opposing orientations and requires the
configured overlap length and mismatch limit. `--correct-overlap` can correct
a disagreement only when one observation is at least Q30 and the other at most
Q14. It copies the stronger base and its quality to the weaker observation,
following fastp 1.4.0. The former `--correction-delta` option is removed and
produces an explanatory error.

`--merge --merged-out merged.fq.gz` includes that correction automatically,
then retains R1 bases and qualities through the overlap and appends the
non-overlapping reverse-complemented R2 tail. It does not calculate a new
consensus quality. For a negative-offset overlap (a short insert with read-through),
it writes the overlapping R1 prefix. Explicit `--correct-overlap=false` conflicts
with merging and is rejected. Both correction and merge are off by default.

These per-base rules follow fastp; SOAPnuke's overlap search and upstream
filter order remain unchanged. SOAPnuke preserves read names and its existing
case-insensitive canonical-base matching/case-preserving output support.
They are not promises of identical whole-pipeline output from both tools.
Both operations require `both-pass`. By default, unmerged accepted pairs go to the regular mate outputs.
Use `--unmerged-mode discard` only when that loss of pairs is intended.

`--min-insert-length` rejects confidently short inserts. Pairs without a usable
overlap are not rejected by that option. Correction is optional outside merging; do not assume that enabling it
automatically improves downstream variant accuracy.

## UMI/barcode labels and retained regions

`--extract-label umi@r1:1:8` captures the first eight bases of R1 as a label.
Coordinates are 1-based and refer to the original read, before trimming.
`--label-remove` removes captured bases from the processed read. Label quality
and N limits are controlled by `--label-min-q` and `--label-max-n`.

`--retain-r1 9:100` keeps that inclusive original-coordinate region. Label
capture and retained-region selection are useful when a library has a known
layout. They do not perform barcode-based demultiplexing or UMI consensus
generation.

## Reference screening

With `--screen-reference reference.fa`, the native method uses exact k-mer
matches. `--screen-k` selects k; `--screen-min-hits` controls the required
distinct hits per reference. The default action, `inspect`, reports evidence
without removing reads.

For `--screen-action remove` or `retain`, explicitly choose
`--screen-unscorable keep` or `reject`. `--screen-pair any` or `both` defines
how mate-level evidence becomes a pair-level hit. These choices can change
which pairs are retained.

`--screen-method supplied-hits --screen-hits hits.tsv` consumes existing hit
calls and requires a matching `hits.tsv.provenance.json`. SOAPnuke does not
launch an aligner or create those calls automatically. Native exact k-mer
screening is not a general alignment or taxonomic classification method.

## Small-RNA profile

`--profile srna` provides a single-end adapter-based flow. Supply an explicit
3′ adapter with trimming enabled, and choose `--srna-min-insert` as needed.
Optional controls reject 5′ adapter matches or poly-A runs.

This profile has its own processing order. It cannot be combined with ordinary
fixed/quality/tail trimming, label capture, adapter inference or reference
filtering. It is not a replacement for downstream small-RNA annotation or
quantification, and is not a full port of the old `filtersRNA` module.

## Reports, repetition and resources

JSON reports are always required; `--html-report` adds a summary. In QC-only
mode, acceptance and output populations are unavailable because no filtering
or FASTQ writing occurs.

`--repetition` counts exactly repeated reads/pairs in an input prefix controlled
by `--repetition-units`. `--overrep-fraction` can flag frequent sequences in
that prefix. The result is a prefix measurement, not automatically a whole-file
estimate; it does not remove duplicates or identify PCR duplicates by alignment.

`--decoder auto` chooses the input decoder. Explicit `rapidgzip` requires a
seekable gzip file and at least 14 threads; stdin uses zlib. `--memory-mib`
limits additional managed buffers, **not total peak process RSS**. Set thread
counts within the CPU allocation and measure resource use with the actual
enabled operations.

## Canonical-base case and resources

Overlap selection/correction/merge and reference indexing/query compare A/C/G/T without case sensitivity. Case-only canonical agreement causes no correction or quality loss; merged agreement keeps R1 spelling and max numerical Q. Output spelling remains original unless an enabled transformation changes bases. N/IUPAC are not wildcards. Adapter resources accept only A/C/G/T in either case; reference FASTA retains IUPAC but only canonical windows match. See [resources](RESOURCES.md) and [module help](process-help.txt).
