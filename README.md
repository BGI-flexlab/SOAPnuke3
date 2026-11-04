# SOAPnuke 3.0.0

SOAPnuke processes short-read FASTQ data with a Rust core and native compression libraries. It provides five commands and an owned Rust library.

**Explore the [interactive visual guide](https://bgi-flexlab.github.io/SOAPnuke3/)** to see each processing step, adjust examples, and inspect option behavior. For offline use, [download the guide ZIP](https://bgi-flexlab.github.io/SOAPnuke3/SOAPnuke-guide.zip), extract it, and open `index.html`. The same files are in [`docs/interactive/`](docs/interactive/).

| Command | Purpose |
|---|---|
| `process` | Trim and filter single or paired reads; inspect references; correct or merge mates; produce QC. |
| `demux` | Route reads to samples using inline, index, or declared tag barcodes. |
| `dedup` | Group complete sequences and retain the earliest representative in input order. |
| `report` | Render saved reports as HTML/TSV or create supported named exports. |
| `resources` | List, show, or verify explicitly supplied local adapter/reference catalogues. |

This repository is separate from [SOAPnuke v1/v2](https://github.com/BGI-flexlab/SOAPnuke). Interfaces, defaults and supported workflows differ; see the [migration guide](docs/MIGRATION.md).

## Install or build

Choose the download matching `uname -m`:

| Linux machine | Download |
|---|---|
| `x86_64` (Intel/AMD) | [Linux x86-64 executable](https://github.com/BGI-flexlab/SOAPnuke3/releases/download/v3.0.0/SOAPnuke-3.0.0-linux-x86_64.tar.gz) |
| `aarch64` (ARM64 server) | [Linux ARM64 executable](https://github.com/BGI-flexlab/SOAPnuke3/releases/download/v3.0.0/SOAPnuke-3.0.0-linux-aarch64.tar.gz) |

Extract the archive and run `./bin/SOAPnuke --version` inside it. Checksums are on the [release page](https://github.com/BGI-flexlab/SOAPnuke3/releases/tag/v3.0.0). The ARM64 download is tested natively on Ubuntu 24.04 and requires **glibc 2.39+** plus the C++ runtime and zlib. Build from source on older Linux systems. Linux ARM64 is not a macOS or 32-bit ARM build.

For source builds, use Rust/Cargo 1.99.1, C, C++17, CMake, Make and zlib development headers. Python 3.10+ is needed for build identity and tests. A minimum Rust version has not been established. Locked Rust dependencies, libdeflate and rapidgzip sources are included; Cargo downloads and submodule initialization are unnecessary. Follow the [step-by-step Linux ARM64 instructions](docs/BUILDING.md#linux-arm64-source-build), or use the same native build commands on Linux x86-64:

```sh
make JOBS=2
./target/release/SOAPnuke --version
export PATH="$PWD/target/release:$PATH"
```

See [building and tests](docs/BUILDING.md). Build jobs and processing threads are separate settings. Portable or byte-identical binary rebuilds are not promised.

**Conda packages are available for Linux x86-64 and ARM64:** follow the [installation instructions](docs/BUILDING.md#install-with-conda). The package is named `soapnuke3` and installs the command `SOAPnuke`. [Bioconda submission](https://github.com/bioconda/bioconda-recipes/pull/71175) is awaiting review; the package is not yet available from that channel. The same guide includes [building the Conda packages from source](docs/BUILDING.md#conda-package-builds).

## Quick start

Create an output directory and supply your reads:

```sh
mkdir clean-output
SOAPnuke process -1 reads_R1.fq.gz -2 reads_R2.fq.gz \
  -C clean_R1.fq.gz -D clean_R2.fq.gz -o clean-output -T 16 \
  -l 20 -q 0.5 -n 0.05 -4 30 --html-report sample.qc.html
```

The JSON report is `clean-output/SOAPnuke.json`. These thresholds are examples, not universal sequencing recommendations. Adapters are trimmed only when supplied or inferred. Existing outputs are not overwritten. For authored sample data, see [examples](examples/README.md). Run `SOAPnuke COMMAND --help` for grouped options, defaults and examples.

## Important behavior

- Filters are off until enabled; minimum read length is one base. Input and output quality encoding default to Phred+33.
- Low quality means **Q < cutoff**. A read fails a fraction limit only when **fraction > limit**; equality passes.
- Canonical A/C/G/T overlap and reference matching is case-insensitive. Read spelling and numeric qualities remain unchanged unless an enabled transformation changes them. N/IUPAC are not wildcards.
- Adapter FASTA accepts A/C/G/T in either case. Reference FASTA retains its IUPAC alphabet, but only canonical windows match.
- Paired-end `--adapter-overlap` is optional and off by default. It requires supplied 3′ adapters for both mates and a unique, supported overlap. See [adapter and processing rules](docs/USAGE.md).
- A resource catalogue is optional for direct adapter/reference paths. `id:ID` and the `resources` actions require an explicit catalogue. Nothing is downloaded or selected implicitly.
- `dedup` needs the full input and a scratch directory. Exact sequence equality is a computational duplicate definition, not molecular duplicate truth.

## Documentation and validation

- [Process and algorithm guide](docs/USAGE.md), [Demux/Dedup/report guide](docs/JOBS.md), [Rust API](docs/API.md), [resources](docs/RESOURCES.md), [architecture](docs/ARCHITECTURE.md)
- [Build and test](docs/BUILDING.md), [dependency integration](docs/DEPENDENCY-INTEGRATION.md), [validation scope](docs/VALIDATION.md), [changes](CHANGELOG.md)
- Complete help: [process](docs/process-help.txt), [demux](docs/demux-help.txt), [dedup](docs/dedup-help.txt), [report](docs/report-help.txt), [resources](docs/resources-help.txt)

```sh
make test JOBS=2 TEST_THREADS=2
```

The maintained route runs native tests, portable CLI suites and owned-library examples. It does not establish full-WGS, all-parameter, scaling, or downstream biological qualification. [Validation](docs/VALIDATION.md) documents test coverage, datasets and measured costs.

## License

See [COPYING](COPYING) and [third-party licenses](THIRD_PARTY.md). Report software problems through the repository issue tracker with the version, explicit options and a minimal shareable example.
