import unittest

import nimarrow

type
  MyType* = object
    a*: string
    b*: int32
    c*: uint8

declareTypedTable(MyType)

test "can construct table from arrays":
  let field1 = newArrowField("a", TypeTag[int32]())
  let field2 = newArrowField("b", TypeTag[string]())

  let data1 = newArrowArray(@[1'i32, 2'i32, 3'i32])
  let data2 = newArrowArray(@["first", "second", "third"])

  let schema = newArrowSchema(@[field1, field2])

  let tableBuilder = newArrowTableBuilder(schema)
  tableBuilder.add data1
  tableBuilder.add data2
  let table = tableBuilder.build

  check table.len == 3

test "can build typed tables":
  let typedBuilder = newTypedBuilder(TypeTag[MyType]())
  typedBuilder.add MyType(a: "a", b: 1'i32, c: 0'u8)
  typedBuilder.add MyType(a: "b", b: 2'i32, c: 1'u8)
  typedBuilder.add MyType(a: "c", b: 3'i32, c: 2'u8)
  let table = typedBuilder.build

  check table.len == 3
  echo $table

