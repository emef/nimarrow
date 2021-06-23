import unittest

import nimarrow

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