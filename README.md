[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) ![Stability](https://img.shields.io/badge/stability-experimental-orange.svg)

# nimarrow - libarrow bindings for nim

[API Documentation](https://emef.github.io/nimarrow/theindex.html)

"[Apache Arrow](https://arrow.apache.org/) defines a language-independent columnar memory format for flat and hierarchical data, organized for efficient analytic operations on modern hardware like CPUs and GPUs. The Arrow memory format also supports zero-copy reads for lightning-fast data access without serialization overhead."

`nimarrow` provides an ergonomic nim interface to the lower level libarrow c api. 

# Project Status

This library is still a WIP and will be developed alongside the [nimarrow_glib](https://github.com/emef/nimarrow_glib/) library which exposes the libarrow-glib c API.

- [x] arrays
- [ ] tables
- [ ] parquet read/write
- [ ] IPC format
- [ ] cuda
