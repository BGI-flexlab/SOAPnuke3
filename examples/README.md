# Synthetic quick-start data

These small files are artificial examples, not reads from people or a
sequencing experiment. Qualities use Phred+33. `reads_R1.fq.gz` and
`reads_R2.fq.gz` contain six pairs; `reads.fq.gz` contains one adapter example.
Compressed-file checksums are recorded in `SHA256SUMS.json`.

For the main README's demo command (`-l 20 -q 0.5 -n 0.05 -4 30`), both mates
must pass. R2 reads have Q40, contain no Ns and have their corresponding R1
lengths. The R1 cases are:

| Pair name, before `/1` or `/2` | R1 condition | Expected result |
|---|---|---|
| `quality-equal` | 40 bases at Q20 | Keep: Q20 is not below Q20 |
| `fraction-equal` | 20 of 40 bases at Q19; the rest Q30 | Keep: low-quality fraction equals 0.5 |
| `fraction-above` | 21 of 40 bases at Q19; the rest Q30 | Reject: low-quality fraction exceeds 0.5 |
| `n-equal` | 2 N bases in 40 bases, Q30 | Keep: N fraction equals 0.05 |
| `n-above` | 3 N bases in 40 bases, Q30 | Reject: N fraction exceeds 0.05 |
| `short` | 29 bases, Q30 | Reject: length is below 30 |

Expected clean order: `quality-equal`, `fraction-equal`, `n-equal`, with the
original mate suffixes, sequences and qualities. The JSON report should have
`status: complete`. No sampling, trimming or adapter inference is enabled by
this example.

The single-end file contains 60 C bases followed by `AGATCGGAAGAGC`, all at Q40.
The README's adapter-trimming command retains exactly the 60 C bases when this
file is supplied as `reads.fq.gz`.
