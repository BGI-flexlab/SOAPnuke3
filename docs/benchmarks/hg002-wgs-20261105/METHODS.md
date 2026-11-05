# HG002 WGS benchmark: methods and provenance

Frozen 5 November 2026. This directory accompanies the six-tool figures and tables. It contains measured results, not predicted performance. No new preprocessing or variant calling was performed to create these presentation files.

## Input

HG002 / NA24385, public accession **SRR37356337**, NovaSeq X paired-end WGS, Phred+33. The complete input contains **430,952,721 pairs**, **129,115,839,913 sequence bases**, and **58,144,003,587 compressed bytes** across both files. Observed read lengths are **35–150 bases**. Dividing the sequence bases by the 3,105,715,063 bases of the complete alignment reference gives **41.57×** nominal depth; this is not mapped coverage or depth restricted to GIAB regions.

The input files and publisher download URLs/checksums are in [input_fastq.tsv](data/input_fastq.tsv). Reference, known-site and truth resource URLs/checksums are in [resources.tsv](data/resources.tsv).

## Implementations

The selected versions are SOAPnuke 1.6.5, SOAPnuke 2.1.10, formal SOAPnuke 3.0.0, fastp 1.4.0, Trimmomatic 0.41 and Cutadapt 5.2. [Software identities](data/preprocessing_software.tsv) pin the executable/launcher hashes and source/runtime identities. Trimmomatic's JAR SHA-256 is `63382643321833f844ebe6099cfd95d9cb653832d9624d793b8643d6bb56793f`; its runtime was Java 25.0.5+8. Cutadapt used CPython 3.13.13.

The **SOAPnuke 2.1.10** results use the validated maintenance-fix implementation. The measured executable carried the earlier banner `2.1.9+percentile-fix+qc-length-fix`, SHA-256 `d47c1994a6f45790075d565e5bb0b300e0e5652aa421faad58972be8f4b2ea13`, source commit `569c46138b8f4a46feaa6164ef861c0e4740ddac`. The prepared 2.1.10 source commit is `5fcd4045a7b7196cb1efe8dd6d624e8b234e7818`; its production-code difference is version metadata only. The newly named executable has SHA-256 `397ab70bd10dfb7d85db96de9773529e36ead7b757979d478fd1dc3f5f28a74e`. No timing measurement was repeated merely to change the banner. [The identity mapping](data/v2_release_identity.json) preserves both identities, and the [maintenance patch against public v2.1.9](data/v2.1.9-report-fixes.patch) is included. Original benchmark records remain unchanged.

## Hardware and timing

Shared Linux host, two Intel Xeon Gold 6442Y sockets, 48 physical cores / 96 logical CPUs. Inputs and outputs were on the same **Lustre** filesystem. No SSD measurements are mixed into these tables.

Each process was confined to 8, 16 or 32 logical CPUs corresponding to distinct physical cores, balanced across sockets. Affinity restricts placement but does not reserve cores. Earlier four-tool masks and later SOAPnuke3/fastp supplement masks differ:

| Cores | Earlier four tools | Formal SOAPnuke3 / fastp 1.4.0 |
| --- | --- | --- |
| 8 | `0-3,24-27` | `2,3,4,5,27,30,31,37` |
| 16 | `0-7,24-31` | `2,3,4,5,6,7,8,9,27,30,31,37,38,41,43,44` |
| 32 | `0-15,24-39` | `2,3,4,5,6,7,8,9,10,11,13,14,15,16,17,19,26,27,29,30,31,32,34,35,36,37,38,40,41,43,44,45` |

Measurements ran sequentially. External validators were outside the timed interval and did not compete with the updated-tool measurements. The table combines 36 accepted historical observations for the unchanged four tools and 18 supplement observations for formal SOAPnuke3 and fastp 1.4.0. These were not one contemporaneous randomized six-tool block. The first supplement set was validated after its six measurements; later supplement runs interleaved validation/cleanup, so cache cadence differed. The SOAPnuke3 T8 observations were 637.58, 680.48 and 996.13 seconds; the slow accepted observation is retained.

Wall time includes startup, input, QC, output compression/writing and native reports. GNU time supplies user/system time and process maximum RSS. CPU percentage is `100 × (user + system) / wall`; effective cores are CPU percentage / 100. Throughput is `58,144.003587 decimal MB / wall seconds`; it is compressed-input throughput, not pairs/s. RSS GiB uses 2^30 bytes; GNU time's KiB value is divided by 1,048,576. It is a process high-water value, not a sum over separately sampled child processes. Median metrics are calculated independently. The figure shows all observations and median lines; it does not use confidence intervals.

## Preprocessing profiles

No adapter removal, poly-G trimming, overlap filtering, deduplication or sampling was enabled in this WGS comparison. Tools retain different filtering/trimming definitions:

| Tool | Quality action | N decision | Minimum length |
| --- | --- | --- | --- |
| SOAPnuke 1.6.5 / 2.1.10 | Reject low-quality fraction ≥0.5; low quality is Q≤5 | Reject N fraction ≥0.05 | 30 |
| SOAPnuke 3.0.0 | Reject low-quality fraction >0.5; low quality is Q<6 | Reject N fraction >0.05 | 30 |
| fastp 1.4.0 | Reject unqualified fraction >50%; unqualified is Q<6 | Reject >7 Ns | 30 |
| Trimmomatic 0.41 | LEADING/TRAILING trimming below Q20 | Retain 0–7 Ns after trimming | 30 after trimming |
| Cutadapt 5.2 | Q20 cumulative-score end trimming | Reject N fraction >0.05 after trimming | 30 after trimming |

These are practical explicit WGS profiles, not mathematically identical workloads. The inclusive/strict 50% quality boundary explains the five-pair SOAPnuke v2/v3 difference. Seven Ns approximate 5% only for 150-base reads, while the input includes shorter reads. Trimmomatic/Cutadapt change read ends. Counts alone are not proof of identical content.

SOAPnuke3, fastp, Trimmomatic and Cutadapt used gzip level 4. Older SOAPnuke retained its native settings: v1 default zlib compression, v2 level 2. Equal numeric levels across libraries do not imply equal compression work. Batching was native: v2's omitted patch resolves to 2,500 × threads. Retained T32 output sizes are included in [RESULTS.md](RESULTS.md).

### Command templates

These arrays preserve the executed settings, replacing host paths and launcher names with portable variables. They are documentation, not a campaign launcher. The executable names `SOAPnuke-v1.6.5`, `SOAPnuke-v2.1.9-report-fixed` and `SOAPnuke3` denote aliases or paths to the three separate frozen SOAPnuke binaries; they are not all installed command names. Replace the first token with your exact binary path. Install the exact pinned versions, set paths to **fresh per-tool/per-run destinations**, and apply the appropriate recorded CPU mask. The outer timed invocation was `taskset -c "$MASK" /usr/bin/time -v COMMAND ...`; the tables also retain the measured user/system seconds.

```bash
R1=/path/to/SRR37356337_1.fastq.gz
R2=/path/to/SRR37356337_2.fastq.gz
OUT=/path/to/fresh/run/output
T=32
mkdir -p "$OUT"
PHRED_CONF="$OUT/phred33.conf"
printf 'qualSys=2\noutQualSys=2\n' > "$PHRED_CONF"
```

#### SOAPnuke 1.6.5

```bash
SOAPnuke-v1.6.5 \
  filter -1 "$R1" -2 "$R2" -C \
  clean.R1.fastq.gz -D clean.R2.fastq.gz -o "$OUT" -T \
  "$T" -l 5 -q 0.5 -n \
  0.05 --minLen 30 -Q 2 -G
```

#### SOAPnuke 2.1.10

```bash
SOAPnuke-v2.1.9-report-fixed \
  filter -1 "$R1" -2 "$R2" -C \
  clean.R1.fastq.gz -D clean.R2.fastq.gz -o "$OUT" -T \
  "$T" -l 5 -q 0.5 -n \
  0.05 -4 30 -c "$PHRED_CONF"
```

#### SOAPnuke 3.0.0

```bash
SOAPnuke3 \
  process --r1 "$R1" --r2 "$R2" --out1 \
  "$OUT/clean.R1.fastq.gz" --out2 "$OUT/clean.R2.fastq.gz" --report "$OUT/qc.json" --input-phred \
  33 --output-phred 33 --threads "$T" --decoder \
  auto --low-q 6 --max-low-q-fraction 0.5 --max-n-fraction \
  0.05 --min-length 30 --pair-policy both-pass --gzip-level \
  4
```

#### fastp 1.4.0

```bash
fastp \
  -i "$R1" -I "$R2" -o "$OUT/clean.R1.fastq.gz" \
  -O "$OUT/clean.R2.fastq.gz" -w "$T" -A -G \
  --dont_eval_duplication -q 6 -u 50 -n \
  7 -l 30 -j "$OUT/fastp.json" -h \
  "$OUT/fastp.html"
```

#### Trimmomatic 0.41

```bash
trimmomatic \
  PE -threads "$T" -phred33 -validatePairs -compressLevel \
  4 -summary "$OUT/summary.txt" "$R1" "$R2" "$OUT/clean.R1.fastq.gz" \
  "$OUT/singleton.R1.fastq.gz" "$OUT/clean.R2.fastq.gz" "$OUT/singleton.R2.fastq.gz" LEADING:20 TRAILING:20 BASECOUNT:N:0:7 \
  MINLEN:30
```

#### Cutadapt 5.2

```bash
cutadapt \
  -j "$T" --quality-base 33 -q 20,20 \
  --max-n 0.05 --minimum-length 30 --pair-filter any \
  --compression-level 4 --json "$OUT/qc.json" -o "$OUT/clean.R1.fastq.gz" \
  -p "$OUT/clean.R2.fastq.gz" "$R1" "$R2"
```

## Downstream workflow

Only each tool's retained **T32 paired outputs** entered calling. Trimmomatic's 37,842 singleton reads were excluded. Each input representation was analyzed independently as HG002; the six inputs were not jointly genotyped as different samples.

```text
paired cleaned FASTQ
  → BWA-MEM2 → coordinate sort/index
  → GATK MarkDuplicates (mark, do not remove)
  → BaseRecalibrator → ApplyBQSR
  → 26 whole-contig HaplotypeCaller GVCF shards
  → ordered GatherVcfs → one GenotypeGVCFs invocation
  → separate SNP/INDEL hard filters → MergeVcfs
  → hap.py / RTG vcfeval → precision, recall and F1
```

| Component | Frozen version |
| --- | --- |
| BWA-MEM2 | Official v2.3 release asset; executable reports 2.2.1 |
| GATK | 4.6.2.0; HTSJDK 4.2.0; Picard 3.4.0 |
| Java | Temurin 17.0.21+9 |
| samtools / HTSlib | 1.18 / 1.17 |
| bcftools / HTSlib | 1.17 / 1.17 |
| hap.py | 0.3.15, build py27hcb73b3d_0, Python 2.7.15 |
| RTG Tools | 3.13 |

Reference: NCBI GRCh38 no-alt plus hs38d1 with UCSC contig names, 2,580 contigs. BQSR used 1000G phase-1 high-confidence hg38 SNPs and Mills/1000G gold-standard hg38 indels. Evaluation used GIAB HG002 GRCh38 v4.2.1 autosomal truth and its fixed `benchmark_noinconsistent.bed`: 481,622 intervals spanning 2,542,242,843 bases. Exact files and hashes are in [resources.tsv](data/resources.tsv).

### Alignment and recalibration settings

There were 16 physical cores per analysis, with at most three analyses concurrently. BWA used 15 threads and read group `@RG\tID:HG002.WGS\tSM:HG002\tPL:ILLUMINA`; samtools sort used `-@ 1`. No BWA `-K` override was supplied. MarkDuplicates used `--REMOVE_DUPLICATES false --CREATE_INDEX true`. BaseRecalibrator used both known-site VCFs above; ApplyBQSR used its resulting recalibration table. Java used `-XX:ActiveProcessorCount=16 -Xmx32g` and per-analysis temporary directories. OMP, OpenBLAS and MKL thread limits were one.

### Calling settings

All reference contigs were partitioned in reference order into 26 shards: contigs at least 10 Mb occupied an individual shard; consecutive smaller contigs were grouped up to 10 Mb. There were no cuts inside a contig and no calling restriction to the confident BED. [The complete partition](data/calling_contig_partition.tsv) is included. At most 16 HaplotypeCaller jobs ran concurrently per analysis, each with one CPU, `-Xmx4g`, `-XX:ActiveProcessorCount=1`, `-XX:ParallelGCThreads=1`, `-ERC GVCF` and `--native-pair-hmm-threads 1`. Diploid defaults were retained. GVCFs were gathered in dictionary order and indexed; **one** GenotypeGVCFs invocation produced each analysis's raw VCF.

### Hard filters and evaluation

SNP and INDEL branches were selected separately with SelectVariants. Each expression below was applied as a separate named VariantFiltration filter, then the branches were combined with MergeVcfs:

| Branch | Expressions |
| --- | --- |
| SNP | `QD < 2.0`; `QUAL < 30.0`; `SOR > 3.0`; `FS > 60.0`; `MQ < 40.0`; `MQRankSum < -12.5`; `ReadPosRankSum < -8.0` |
| INDEL | `QD < 2.0`; `QUAL < 30.0`; `FS > 200.0`; `ReadPosRankSum < -20.0` |

Missing annotations pass their individual expression under the recorded GATK behavior. The workflow used hard filters, not VQSR. Other query variant types excluded by SNP/INDEL selection were not restored; the truth denominator remained fixed.

hap.py used `--engine=vcfeval --pass-only --threads 16`, the same GRCh38 FASTA, GIAB truth and BED for every tool, a reference RTG SDF, and RTG memory 24G. The selected rows from `happy.extended.csv` were unstratified PASS SNP/INDEL rows with `Subtype=*`, `Subset=*`, `Genotype=*`, `QQ.Field=QUAL`, `QQ=*`.

```text
precision = QUERY.TP / (QUERY.TP + QUERY.FP)
recall    = TRUTH.TP / (TRUTH.TP + TRUTH.FN)
F1        = 2 × precision × recall / (precision + recall)
```

Truth denominators were 3,365,127 SNPs and 525,469 indels under the evaluator's representation/counting convention. Truth and query TP may differ because of equivalent haplotype representations. Calls outside assessed regions are UNK rather than automatically FP. [Unrounded scores and counts](data/variant_calling_accuracy.tsv) and [VCF record counts](data/downstream_record_counts.tsv) are included.

### Identity, validation and interpretation

Formal SOAPnuke 3.0.0 and fastp 1.4.0 T32 retained gzip files were **byte-identical** to the corresponding FASTQs already used in the completed calling analyses, for both mates. Calling results were therefore reused by exact input identity; these are not newly executed calling runs. [Calling input identities](data/calling_input_lineage.tsv) and [retained output hashes](data/retained_fastq_hashes.tsv) document the binding.

The 18 updated-tool outputs received complete gzip/FASTQ/mate/count/base/hash scans and same-tool QC comparisons. The unchanged tools retain their original validation depth: retained T32 outputs were fully scanned; other timing duplicates used the recorded execution/native-report checks. This presentation does not retrospectively claim complete scans for those historical duplicates. Every original calling analysis passed its saved stage-exit and BAM/VCF/sample checks.

SOAPnuke v1 and v2 retained the same paired-content multiset but different order. BWA's default batching/insert-size estimation produced slightly different downstream alignments. Tiny score differences therefore cannot be attributed solely to different QC decisions. No order-controlled downstream rerun or accuracy confidence interval is asserted.

This is one WGS sample, one hardware/filesystem environment and explicit profiles with different tool semantics. It supports the reported preprocessing performance and closely similar observed calling metrics; it does not establish universal speed leadership, noninferiority, or biological equivalence.

## Figures and data

- [All 54 observations](data/preprocessing_54_observations.tsv)
- [All 18 median cells and ranges](data/preprocessing_medians.tsv)
- [Complete readable results](RESULTS.md)
- [Input identities](data/input_fastq.tsv), [resource identities](data/resources.tsv), [software identities](data/preprocessing_software.tsv)
- [Retained counts and bases](data/retained_output_counts.tsv), [retained hashes](data/retained_fastq_hashes.tsv)
- [Accuracy counts](data/variant_calling_accuracy.tsv), [calling input binding](data/calling_input_lineage.tsv)

Run `python3 figures/plot_results.py` with Python, Matplotlib and NumPy to regenerate the PNG/SVG/PDF figures from this directory's TSVs. The plot generator creates its cache and temporary directory below the benchmark directory.
