# Copyright (C) Marc Azar. All rights reserved.
# MIT License. Look at LICENSE.txt for more info
type 
  Units* = SomeUnsignedInt
  Bit = range[0..1]
  BitVector*[T: Units] = object
    base*: seq[T]
    bitlength: int

# Forward declarations
func `len`*[T](b: BitVector[T]): int {.inline.}
func cap*[T](b: BitVector[T]): int {.inline.}
func `[]`*[T](b: BitVector[T], i: int): Bit {.inline.}

func newBitVector*[T](size: int, init = 0): BitVector[T] {.inline.} =
  ## Create new in-memory BitVector of type T and number of elements is
  ## `size` rounded up to the nearest byte. You can initialize the
  ## bitvector to 1 by passing any value other than zero to init.
  ##
  var blocks = size div (T.sizeof * 8)
  if blocks == 0 : blocks = 1
  elif (size mod (T.sizeof * 8)) > 0 : blocks += 1
  result.base = newSeqOfCap[T](blocks)
  result.base.setlen(blocks)
  result.bitlength = size * 8
  if init != 0:
    for i in 0 ..< size:
      result.base[i] = 1

func `[]`*[T](b: BitVector[T], i: int): Bit {.inline.} =
  assert(i < b.cap and i >= 0, "Index out of range")
  b.base[i div (T.sizeof * 8)] shr (i and (T.sizeof * 8 - 1)) and 1

func `[]=`*[T](b: var BitVector[T], i: int, value: Bit) {.inline.} =
  assert(i < b.cap and i >= 0, "Index out of range")
  var w = addr b.base[i div (T.sizeof * 8)]
  if value == 0:
    w[] = w[] and not (1.T shl (i and (T.sizeof * 8 - 1)))
  else:
    w[] = w[] or (1.T shl (i and (T.sizeof * 8 - 1)))

func add*[T](b: var BitVector[T], value: Bit) {.inline.} =
  ## Add an element to the end of the BitVector.
  let i = b.bitlength  
  if (i div (T.sizeof * 8)) >= b.base.len():
    b.base.add 0.T

  b[i] = value
  b.bitlength += 1      

func cap*[T](b: BitVector[T]): int {.inline.} =
  ## Returns capacity, i.e number of bits
  b.len * (T.sizeof * 8)

func `len`*[T](b: BitVector[T]): int {.inline.} =
  ## Returns length, i.e number of elements
  b.base.len()

func `==`*(x, y: Bitvector): bool =
  x[0 .. (x.cap - 1)] == y[0 .. (y.cap - 1)]

func `$`*[T](b: BitVector[T]): string {.inline.} =
  ## Prints number of bits and elements the BitVector is capable of handling.
  ## It also prints out a slice if specified in little endian format.
  result =
    "BitVector with capacity of " & $b.cap & " bits and " & $b.len &
      " unique elements"