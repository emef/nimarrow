import options
import unittest

import nimarrow

test "can construct simple arrays":
  let arr = newArrowArray[int32](@[1'i32, 2'i32, 3'i32])
  check arr[0] == 1'i32

  check @arr[1..2] == @[2'i32, 3'i32]

  let arr2 = newArrowArray[float32](@[1'f32, 2'f32, 3'f32])
  check arr2[0] == 1'f32

  check @arr2[1..2] == @[2'f32, 3'f32]

  let arr3 = newEmptyArrowArray[int32]()
  check arr3.len == 0
  check @arr3 == newSeq[int32]()

  let arr4 = newArrowArray(@[some(1'i64), none(int64), some(2'i64), 
                             none(int64), none(int64)])
  check arr4.len == 5
  check @arr4 == @[1'i64, 0'i64, 2'i64, 0'i64, 0'i64]
  check arr4.isNullAt(1)
  check arr4.isNullAt(3)
  check arr4.isNullAt(4)

test "can build arrays with builder":
  let builder = newArrowArrayBuilder[int64]()
  builder.add 1'i64
  builder.add 2'i64 
  builder.add(3'i64)
  builder.add(none(int64))
  let arr = builder.build()

  check arr.len == 4
  check @arr == @[1'i64, 2'i64, 3'i64, 0'i64]
  check arr.isNullAt(3)
