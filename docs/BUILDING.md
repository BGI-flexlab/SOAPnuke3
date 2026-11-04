# Installation, builds and portable tests

## Linux ARM64 source build

Build natively on the ARM64 server. `uname -m` should print `aarch64` (also called ARM64); an x86-64 executable cannot run natively there. Linux ARM64 support does not imply macOS or 32-bit ARM support.

The 3.0.0 source passed `make test` on a [native Ubuntu 24.04 ARM64 runner](https://github.com/BGI-flexlab/SOAPnuke3/actions/runs/41795521134), including the slow test, command-line checks and owned-library examples. The [ARM64 download](https://github.com/BGI-flexlab/SOAPnuke3/releases/download/v3.0.0/SOAPnuke-3.0.0-linux-aarch64.tar.gz) comes from that successful build. It requires glibc 2.39+, `GLIBCXX_3.4.30`, `CXXABI_1.3.13`, and zlib; compile natively on an older system instead. This establishes functional build support, not ARM performance or scaling claims.

On Ubuntu 24.04 ARM64, install the system build tools:

```sh
sudo apt-get update
sudo apt-get install build-essential cmake zlib1g-dev python3 curl git
```

Install [Rust with rustup](https://rust-lang.org/tools/install/) if it is not already available, then select the tested toolchain for these commands:

```sh
rustup toolchain install 1.99.1 --profile minimal
git clone --depth 1 https://github.com/BGI-flexlab/SOAPnuke3.git
cd SOAPnuke3
rustup run 1.99.1 make JOBS=2
rustup run 1.99.1 make test JOBS=2 TEST_THREADS=2
./target/release/SOAPnuke --version
export PATH="$PWD/target/release:$PATH"
```

The same commands build natively on Linux x86-64. `JOBS` controls build concurrency, not the runtime `--threads` option. Cargo uses the vendored dependencies without downloading crates. The system tools and Rust installation need network access unless already installed. Use a fresh source directory for each architecture; cross-compilation is not the documented route. The maintained checkout includes a synchronization-only unit-test fix also applied by the Conda recipe; processing behavior is unchanged. The original 3.0.0 tag and source archive are preserved.

## Install with Conda

The Linux x86-64 and ARM64 Conda packages passed [native build, functional and fresh-installation tests](https://github.com/BGI-flexlab/SOAPnuke3/actions/runs/41802552250). The [Bioconda recipe submission](https://github.com/bioconda/bioconda-recipes/pull/71175) is awaiting review; `conda install -c bioconda soapnuke3` is **not available yet**. Until channel publication, use the tested package from the GitHub release.

Download the package for the machine where it will run:

| `uname -m` | Conda platform | Package |
|---|---|---|
| `x86_64` | `linux-64` | [soapnuke3-3.0.0-h455febe_1.conda](https://github.com/BGI-flexlab/SOAPnuke3/releases/download/v3.0.0/soapnuke3-3.0.0-h455febe_1.conda) |
| `aarch64` | `linux-aarch64` | [soapnuke3-3.0.0-h5912ae5_1.conda](https://github.com/BGI-flexlab/SOAPnuke3/releases/download/v3.0.0/soapnuke3-3.0.0-h5912ae5_1.conda) |

Check the download against `SHA256SUMS-conda-build1` on the [release page](https://github.com/BGI-flexlab/SOAPnuke3/releases/tag/v3.0.0). Both packages declare glibc 2.17 or newer. They use Conda's C++ and compression libraries; this differs from the standalone ARM archive's glibc 2.39 requirement. The native installation tests ran on Ubuntu 24.04, not every older Linux distribution.

First create an environment with the package's runtime dependencies. This step matters because [installing a local Conda file does not automatically resolve dependencies](https://docs.conda.io/projects/conda/en/latest/user-guide/concepts/installing-with-conda.html#installing-conda-packages-offline):

```sh
conda create -n soapnuke3 --override-channels -c conda-forge \
  'libgcc>=16' 'libstdcxx>=16' 'libzlib>=1.3.2,<2.0a0' zlib
conda activate soapnuke3
```

Then install **one** file matching the current machine, from its download directory:

```sh
# Linux x86-64
conda install --offline ./soapnuke3-3.0.0-h455febe_1.conda
```

```sh
# Linux ARM64
conda install --offline ./soapnuke3-3.0.0-h5912ae5_1.conda
```

Verify the installation with `SOAPnuke --version` and `SOAPnuke process --help`. Neither Rust nor a C/C++ compiler is needed to use these packages.

## Conda package builds

The recipe in [`packaging/conda`](../packaging/conda/) builds the immutable 3.0.0 source archive with Conda's Rust/C/C++ compilers and zlib. It applies a test-only synchronization patch so the CPU-permit contention test does not depend on thread scheduling; processing code and assertions are unchanged. The GitHub packages use Conda build number 1. The first Bioconda submission uses build number 0 as required for a new channel package, with the same source and test patch. The package name is `soapnuke3`; the installed command remains `SOAPnuke`.

To build a package on either a Linux x86-64 or ARM64 machine, use a current checkout containing the recipe:

```sh
conda create -n soapnuke3-packaging --override-channels -c conda-forge conda-build=26.9.1
conda activate soapnuke3-packaging
mkdir -p .scratch/tmp .scratch/conda-bld
export TMPDIR="$PWD/.scratch/tmp"
python -m conda build packaging/conda --override-channels -c conda-forge \
  --no-anaconda-upload --croot "$PWD/.scratch/conda-bld"
conda create -n soapnuke3 --override-channels -c conda-forge \
  -c "file://$PWD/.scratch/conda-bld" soapnuke3=3.0.0
conda activate soapnuke3
SOAPnuke --version
```

The recipe runs native unit tests and the Process, module and adapter-overlap test cases before packaging. Conda then installs the package into a separate test environment and checks the commands and an actual filtering result. A completed local build does not mean the package is already on Bioconda: [Bioconda publication requires review and a successful channel build](https://bioconda.github.io/contributor/workflow.html).

## Maintained test suite

Build on Linux with the tested Rust/Cargo 1.99.1, C/C++17, CMake, Make, zlib headers and Python3.10+. Toolchain/system libraries are external; minimum Rust version and byte-identical/portable binaries are not promised. Rust dependencies and native compression sources are vendored and locked. Large real-data and timing inputs are not needed or included.

```sh
make JOBS=2
make test JOBS=2 TEST_THREADS=2
./scripts/build_api_example.sh
```

`make test` runs the complete native unit suite, the explicitly ignored 120-second parser/transform mutation test, both portable CLI runners, and two actual owned-library examples. `make test-all` is a compatibility alias for that complete route; `make test-slow` runs the deliberately slow tests separately. Scientific assertions are retained. Native tests use repository-owned literals and fresh per-invocation workspaces under `.scratch/tmp/native`, including the previously external header/catalogue/storage controls. No external fixture environment is required.

`tests/fixtures/native/PROVENANCE.json` records the authored computational controls. Three grouping cases share one 343-unit payload; storage limits distinguish the cases. These fixtures are not experimental sequencing evidence.

`make legacy-cli-test` and `make cli-test` remain individually runnable. CLI outputs use fresh repository-local directories. The linked API examples cover mixed-Phred merging and a reference resource ID with actual IO/counters/close/drop. Use a clean build tree so the native archive is unambiguous. Actual external named-consumer qualification and large-data/performance studies remain separate integrations with explicit dependencies; they are not silently represented as unit-test passes.

`scripts/build_identity.py` records compile/dependency source hashes before build. The source manifest records the current compile inputs; a new build's executable/caller hashes and runtime context are separate. For an API caller linked manually, the native link directories must be the actual Cargo build outputs. See the example build script rather than linking an unrelated installed library.

Run `make dependency-check` for locked/vendor/patch/native-ABI identity checks, or `make dependency-integration` for those checks plus the complete native/slow/CLI/actual-library route. See [the dependency upgrade contract](DEPENDENCY-INTEGRATION.md).

## Optional pinned fastp comparison

After building, the following independent integration checks the changed F07/P03/P04 rules against an explicitly supplied **fastp 1.4.0** executable. It generates small authored inputs and retains commands, outputs and comparisons in a new directory; it does not download a tool or dataset.

```sh
python3 tests/compare_fastp.py --soap target/release/SOAPnuke \
  --fastp /path/to/fastp-1.4.0 --out .scratch/tmp/fastp-comparison-new
```

This is separate from `make test` because fastp is an external test dependency. It checks exact decoded sequences and qualities for unique overlaps; it does not assert whole-pipeline or overlap-selector equivalence.
