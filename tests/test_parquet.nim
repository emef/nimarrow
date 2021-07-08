import std/os
import std/unittest

import nimarrow

type
  CustomType* = object
    x*: int32
    y*: string
    z*: uint8

registerTypedTable(CustomType)

test "can read and write parquet":
  let
    path = getTempDir() / "test.parquet"
    schema = newArrowSchema(@[
      newArrowField("a", int32),
      newArrowField("b", string)
    ])

  let
    col1 = newArrowArray(@[1'i32, 2'i32, 3'i32])
    col2 = newArrowArray(@["first", "second", "third"])
    tableBuilder = newArrowTableBuilder(schema)

  tableBuilder.add col1
  tableBuilder.add col2
  let table = tableBuilder.build

  # write the table to a local parquet file
  table.toParquet(path)

  # read an entire parquet file into an ArrowTable
  let rereadTable = fromParquet(path)

  # the re-read table should be the same as the original
  check table == rereadTable

test "can read and write with custom types":
  let
    path = getTempDir() / "typed.parquet"
    expected = @[
      CustomType(x: 0'i32, y: "y", z: 0'u8),
      CustomType(x: 1'i32, y: "yy", z: 10'u8),
      CustomType(x: 2'i32, y: "yyy", z: 100'u8)
    ]

    typedWriter = newTypedParquetWriter[CustomType](path)

  # append each record to the writer
  for x in expected:
    typedWriter.add x

  # important: close the parquet file to write the footer/metadata
  typedWriter.close()

  # re-read the parquet file into custom types
  let reader = newParquetReader(path)
  var reread = newSeq[CustomType]()
  for x in reader.iter(CustomType):
    reread.add x

  # re-read elements should be the same as were written
  check reread == expected
