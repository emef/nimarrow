[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) ![Stability](https://img.shields.io/badge/stability-experimental-orange.svg)

# nimarrow - libarrow bindings for nim

[API Documentation](https://emef.github.io/nimarrow/theindex.html)

"[Apache Arrow](https://arrow.apache.org/) defines a language-independent columnar memory format for flat and hierarchical data, organized for efficient analytic operations on modern hardware like CPUs and GPUs. The Arrow memory format also supports zero-copy reads for lightning-fast data access without serialization overhead."

`nimarrow` provides an ergonomic nim interface to the lower level libarrow c api. 

# Project Status

This library is still a WIP and will be developed alongside the [nimarrow_glib](https://github.com/emef/nimarrow_glib/) library which exposes the libarrow-glib c API.

- [x] arrays
- [ ] date/timestamp types
- [ ] tables
- [ ] parquet read/write
- [ ] IPC format
- [ ] cuda

# Code Samples

## Arrays

```nim
import options
let arr = newArrowArray[int32](@[1'i32, 2'i32, 3'i32])
doAssert arr[0] == 1'i32
doAssert @arr == @[1'i32, 2'i32, 3'i32]

# can take a slice of an existing array, returning a view (no copy).
let s = arr[1..3]
doAssert @s == @[2'i32, 3'i32]

# use array builders to avoid creating a copy of the data, .build()
# transfers ownership of its buffer into the newly-created array.
let builder = newArrowArrayBuilder[int64]()
builder.add 1'i64
builder.add 2'i64
builder.add none(int64)
let withNulls = builder.build()
 
# nulls show up as 0, must check isNullAt(i)
doAssert @withNulls == @[1'i64, 2'i64, 0'i64]
doAssert withNulls.isNullAt(2)
```