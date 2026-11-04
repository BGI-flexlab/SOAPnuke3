# Attribution and bundled dependencies

SOAPnuke v3 continues the SOAPnuke project. The original repository is
[BGI-flexlab/SOAPnuke](https://github.com/BGI-flexlab/SOAPnuke). Its GPLv3
license text is preserved in [COPYING](COPYING); this package is not a
relicensing of that project.

Bundled components retain their own upstream notices:

| Component | Source location | License texts |
|---|---|---|
| libdeflate | `soapnuke-rs/third_party/libdeflate` | [COPYING](soapnuke-rs/third_party/libdeflate/COPYING) |
| rapidgzip | `soapnuke-rs/third_party/rapidgzip` | [MIT](soapnuke-rs/third_party/rapidgzip/LICENSE-MIT), [Apache](soapnuke-rs/third_party/rapidgzip/LICENSE-APACHE) |
| librapidarchive | `soapnuke-rs/third_party/rapidgzip/librapidarchive` | [MIT](soapnuke-rs/third_party/rapidgzip/librapidarchive/LICENSE-MIT), [Apache](soapnuke-rs/third_party/rapidgzip/librapidarchive/LICENSE-APACHE) |
| Rust crates | `vendor/`, pinned by `soapnuke-rs/Cargo.lock` | License files within each vendored crate |

The system zlib and compiler/runtime libraries are build/runtime prerequisites;
their system distributions provide their notices. Nested bundled components
retain any additional notices shipped in their source directories.

The user documentation was organized with reference to the official
[fastp README](https://github.com/OpenGene/fastp#readme),
[Trimmomatic README](https://github.com/usadellab/Trimmomatic#readme), and
[Cutadapt README](https://github.com/marcelm/cutadapt#readme) and
[user guide](https://cutadapt.readthedocs.io/en/stable/guide.html).
These are documentation references, not dependencies or claims of identical
algorithms, defaults or outputs. SOAPnuke commands and semantics in this
package are based on its own source and help text.

The adjacent-change complexity and overlap correction/merge base-quality rules are implemented in Rust with reference to [fastp 1.4.0](https://github.com/OpenGene/fastp/tree/v1.4.0), specifically `filter.cpp`, `basecorrector.cpp` and `overlapanalysis.cpp`. fastp is an external differential-test reference, not a runtime or bundled dependency. See [the exact scope](docs/USAGE.md) and upstream [license](https://github.com/OpenGene/fastp/blob/v1.4.0/LICENSE).
