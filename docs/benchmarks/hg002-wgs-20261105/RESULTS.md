# Complete HG002 WGS results

Data freeze: 5 November 2026. Each preprocessing cell contains three timed observations. Medians are calculated independently for each metric. The elapsed-time range includes every accepted observation.

| Tool | Cores | Runs | Median wall (s) | Wall range (s) | Median CPU (%) | Median peak RSS (GiB) | Median input MB/s | Retained pairs |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| SOAPnuke 3.0.0 | 8 | 3 | 680.48 | 637.58–996.13 | 618.9 | 0.084 | 85.45 | 430,883,998 |
| SOAPnuke 3.0.0 | 16 | 3 | 376.90 | 373.99–393.37 | 1,060.5 | 0.130 | 154.27 | 430,883,998 |
| SOAPnuke 3.0.0 | 32 | 3 | 376.49 | 375.52–379.72 | 1,223.0 | 0.225 | 154.44 | 430,883,998 |
| SOAPnuke 2.1.10 | 8 | 3 | 2,022.21 | 2,013.67–2,036.87 | 770.3 | 0.988 | 28.75 | 430,883,993 |
| SOAPnuke 2.1.10 | 16 | 3 | 1,477.57 | 1,474.86–1,558.49 | 1,549.1 | 2.735 | 39.35 | 430,883,993 |
| SOAPnuke 2.1.10 | 32 | 3 | 1,201.45 | 1,183.10–1,207.62 | 3,135.4 | 8.530 | 48.39 | 430,883,993 |
| SOAPnuke 1.6.5 | 8 | 3 | 11,838.00 | 11,819.00–11,860.00 | 201.0 | 0.624 | 4.91 | 430,883,993 |
| SOAPnuke 1.6.5 | 16 | 3 | 11,831.00 | 11,794.00–11,838.00 | 206.1 | 0.632 | 4.91 | 430,883,993 |
| SOAPnuke 1.6.5 | 32 | 3 | 11,833.00 | 11,826.00–11,851.00 | 234.1 | 0.647 | 4.91 | 430,883,993 |
| fastp 1.4.0 | 8 | 3 | 876.52 | 866.23–905.19 | 565.6 | 0.176 | 66.34 | 430,889,028 |
| fastp 1.4.0 | 16 | 3 | 614.65 | 612.52–639.30 | 825.7 | 0.280 | 94.60 | 430,889,028 |
| fastp 1.4.0 | 32 | 3 | 507.37 | 505.03–507.68 | 1,157.6 | 0.533 | 114.60 | 430,889,028 |
| Trimmomatic 0.41 | 8 | 3 | 15,528.00 | 15,126.00–19,566.00 | 154.9 | 4.067 | 3.74 | 430,889,593 |
| Trimmomatic 0.41 | 16 | 3 | 15,666.00 | 15,481.00–15,780.00 | 159.0 | 5.938 | 3.71 | 430,889,593 |
| Trimmomatic 0.41 | 32 | 3 | 15,475.00 | 15,154.00–15,962.00 | 161.5 | 9.749 | 3.76 | 430,889,593 |
| Cutadapt 5.2 | 8 | 3 | 1,147.96 | 980.97–1,186.35 | 538.5 | 0.123 | 50.65 | 429,492,691 |
| Cutadapt 5.2 | 16 | 3 | 772.07 | 749.50–918.97 | 788.2 | 0.177 | 75.31 | 429,492,691 |
| Cutadapt 5.2 | 32 | 3 | 751.98 | 748.40–790.94 | 806.4 | 0.081 | 77.32 | 429,492,691 |

## Downstream accuracy

| Tool | SNP precision (%) | SNP recall (%) | SNP F1 (%) | INDEL precision (%) | INDEL recall (%) | INDEL F1 (%) |
| --- | --- | --- | --- | --- | --- | --- |
| Cutadapt 5.2 | 99.6849 | 98.0568 | 98.8641 | 99.4418 | 99.0469 | 99.2440 |
| SOAPnuke 3.0.0 | 99.6876 | 98.0504 | 98.8622 | 99.4573 | 99.0279 | 99.2422 |
| SOAPnuke 1.6.5 | 99.6883 | 98.0510 | 98.8629 | 99.4559 | 99.0273 | 99.2411 |
| SOAPnuke 2.1.10 | 99.6875 | 98.0505 | 98.8622 | 99.4568 | 99.0277 | 99.2418 |
| fastp 1.4.0 | 99.6880 | 98.0505 | 98.8625 | 99.4562 | 99.0277 | 99.2415 |
| Trimmomatic 0.41 | 99.6879 | 98.0485 | 98.8614 | 99.4559 | 99.0300 | 99.2425 |

These are one-sample results, not replicated accuracy estimates. See [methods](METHODS.md) for evaluation regions, exact settings and the byte-identity reuse of two calling results.

## Retained data for downstream analysis

| Tool | Paired records | R1 bases | R2 bases | Combined paired output (GB) |
| --- | --- | --- | --- | --- |
| SOAPnuke 1.6.5 | 430,883,993 | 64,548,618,997 | 64,549,431,378 | 62.015 |
| SOAPnuke 2.1.10 | 430,883,993 | 64,548,618,997 | 64,549,431,378 | 68.718 |
| SOAPnuke 3.0.0 | 430,883,998 | 64,548,619,487 | 64,549,431,858 | 61.353 |
| fastp 1.4.0 | 430,889,028 | 64,548,863,124 | 64,549,656,035 | 61.276 |
| Trimmomatic 0.41 | 430,889,593 | 64,525,317,979 | 64,516,023,675 | 64.109 |
| Cutadapt 5.2 | 429,492,691 | 64,174,539,581 | 64,111,723,682 | 59.734 |

Trimmomatic additionally produced 28,531 R1-only reads and 9,311 R2-only reads. Those singletons were excluded from the paired downstream comparison.
