[![nimarrow CI](https://github.com/emef/nimarrow/actions/workflows/ci.yaml/badge.svg)](https://github.com/emef/nimarrow/actions/workflows/ci.yaml) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) ![Stability](https://img.shields.io/badge/stability-experimental-orange.svg)

# nimarrow - libarrow bindings for nim

[API Documentation](https://emef.github.io/nimarrow/theindex.html)

"[Apache Arrow](https://arrow.apache.org/) defines a language-independent columnar memory format for flat and hierarchical data, organized for efficient analytic operations on modern hardware like CPUs and GPUs. The Arrow memory format also supports zero-copy reads for lightning-fast data access without serialization overhead."

`nimarrow` provides an ergonomic nim interface to the lower level libarrow c api.

# Dependencies

`nimarrow` depends on the package `nimarrow_glib` which provides bindings to the  libarrow-glib and libparquet-glib shared libraries. See the [installation notes](https://github.com/emef/nimarrow_glib/#installation-notes) for instructions on how to install those libraries.

# Project Status

This library is still a WIP and will be developed alongside the [nimarrow_glib](https://github.com/emef/nimarrow_glib/) library which exposes the libarrow-glib c API.

- [x] arrays
- [ ] date/timestamp/decimal types
- [x] tables
- [ ] parquet read/write
- [ ] IPC format
- [ ] cuda

# Code Samples

## Arrays

An ArrowArray[T] is simply a 1D array of type T. It manages its own data on the heap in 64byte-aligned buffers to interop with the libarrow-glib c API.

```nim
import options
import nimarrow

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

## Tables

An ArrowTable is an ordered collection of named arrays (columns). Each column name and type is described by its ArrowField, and an ArrowSchema describes all of the columns in a table.

To construct a table, we use an ArrowTableBuilder which is constructed with the intended schema. Each column's data must then be added to the builder in the order specified by the schema. Creating a table does not copy any of the column data, it will share the internal buffers of the arrays used to construct it.

```nim
import nimarrow

# Schema will be (a: int32, b: string)
let field1 = newArrowField("a", TypeTag[int32]())
let field2 = newArrowField("b", TypeTag[string]())
let schema = newArrowSchema(@[field1, field2])

# Column data for the described fields in the schema.
let data1 = newArrowArray(@[1'i32, 2'i32, 3'i32])
let data2 = newArrowArray(@["first", "second", "third"])

# Add each column to the table in order specified by the schema.
let tableBuilder = newArrowTableBuilder(schema)
tableBuilder.add data1
tableBuilder.add data2
let table = tableBuilder.build

# Convert the table into string representation including
# it's metadata and all contents.
discard $table
```