import std/unittest

import nimarrow

type
  MyType* = object
    a*: string
    b*: int32
    c*: uint8

registerTypedTable(MyType)

test "can construct table from arrays":
  let field1 = newArrowField("a", int32)
  let field2 = newArrowField("b", string)

  let data1 = newArrowArray(@[1'i32, 2'i32, 3'i32])
  let data2 = newArrowArray(@["first", "second", "third"])

  let schema = newArrowSchema(@[field1, field2])

  let tableBuilder = newArrowTableBuilder(schema)
  tableBuilder.add data1
  tableBuilder.add data2
  let table = tableBuilder.build

  check table.len == 3
  check table.col(string, "b")[1] == "second"
  check @(table.col(int32, "a")) == @[1'i32, 2'i32, 3'i32]

test "can build typed tables":
  let typedBuilder = newTypedBuilder(MyType)
  typedBuilder.add MyType(a: "a", b: 1'i32, c: 0'u8)
  typedBuilder.add MyType(a: "b", b: 2'i32, c: 1'u8)
  typedBuilder.add MyType(a: "c", b: 3'i32, c: 2'u8)
  let table = typedBuilder.build

  check table.len == 3

test "can iterate over typed table":
  let expected = @[
    MyType(a: "a", b: 1'i32, c: 0'u8),
    MyType(a: "b", b: 2'i32, c: 1'u8),
    MyType(a: "c", b: 3'i32, c: 2'u8)
  ]

  let table = newArrowTable(MyType, expected)
  var rebuilt = newSeq[MyType]()
  for x in table.iter(MyType):
    rebuilt.add x

  check rebuilt == expected

