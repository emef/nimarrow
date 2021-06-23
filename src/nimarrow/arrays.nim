import macros
import options

import nimarrow_glib

import ./bitarray

## An ArrowArray[T] is simply a 1D array of type T. It manages its
## own data on the heap in 64byte-aligned buffers to interop with
## the libarrow-glib c API.
runnableExamples:
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

type
  ArrowArrayObj[T] = object
    offsets: WrappedBuffer[uint32]
    data: WrappedBuffer[T]
    nullBitmap: WrappedBuffer[NullBitmapBase]
    glibArray*: GArrowArrayPtr

  ArrowArray*[T] = ref ArrowArrayObj[T]

  NullBitmapBase = uint32
  NullBitmap* = BitVector[NullBitmapBase]

  WrappedBufferObj[T] = object
    raw: ptr UncheckedArray[T]
    buf: GArrowBufferPtr
    bytes: int64
    length: int64

  WrappedBuffer[T] = ref WrappedBufferObj[T]

  ArrowArrayBuilderObj[T] = object
    offsets: WrappedBuffer[uint32]
    data: WrappedBuffer[T]
    nullBitmap: NullBitmap
    nNulls: int64
    valid: bool

  ArrowArrayBuilder*[T] = ref ArrowArrayBuilderObj[T]

  Bytes* = seq[byte]

  TypeTag*[T] = object


proc isBinary(t: typedesc): bool =
  t is string or t is Bytes

proc reserveBytes[T](b: WrappedBuffer[T], bytes: int64) =
  if b.raw == nil:
    let newBytes = 64 * ((bytes + 64) div 64)
    b.raw = cast[ptr UncheckedArray[T]](alloc(newBytes))
    b.bytes = newBytes
  elif bytes > b.bytes:
    let newBytes = 2 * b.bytes
    b.raw = cast[ptr UncheckedArray[T]](realloc(b.raw, newBytes))
    b.bytes = newBytes

proc reserveElements[T](b: WrappedBuffer[T], elems: int64) =
  if b.raw == nil:
    let newBytes = max(64, 2 * sizeof(T))
    b.raw = cast[ptr UncheckedArray[T]](alloc(newBytes))
    b.bytes = newBytes
  elif elems * sizeof(T) > b.bytes:
    let newBytes = 2 * b.bytes
    b.raw = cast[ptr UncheckedArray[T]](realloc(b.raw, newBytes))
    b.bytes = newBytes

proc addElement[T](b: WrappedBuffer[T], elem: T) =
  b.reserveElements(b.length + 1)
  b.raw[b.length] = elem
  b.length += 1

proc addBytes[T](b: WrappedBuffer[T], data: pointer, size: Natural) =
  b.reserveBytes(b.length + size)
  let dest = cast[ptr UncheckedArray[char]](b.raw)
  copyMem(addr dest[b.length], data, size)
  b.length += size

proc setGlibBuffer[T](b: WrappedBuffer[T]) =
  b.buf = bufferNew(b.raw, b.bytes)

proc newArrowArrayBuilder*[T](): ArrowArrayBuilder[T]
proc add*[T](builder: ArrowArrayBuilder[T], x: T)
proc add*[T](builder: ArrowArrayBuilder[T], x: Option[T])
proc build*[T](builder: ArrowArrayBuilder[T]): ArrowArray[T]

proc `=destroy`*[T](x: var ArrowArrayObj[T]) =
  if x.glibArray != nil:
    gObjectUnref(x.glibArray)

proc `=destroy`*[T](x: var WrappedBufferObj[T]) =
  if x.raw != nil:
    dealloc(x.raw)

  if x.buf != nil:
    gObjectUnref(x.buf)

proc copyToBuffer[T](arr: openArray[T]): WrappedBuffer[T] =
  let bytes = ((sizeof(T) * arr.len + 64) / 64).toInt
  let raw = cast[ptr UncheckedArray[T]](alloc(bytes))
  for i, x in arr:
    raw[i] = x

  WrappedBuffer[T](
    raw: raw,
    bytes: bytes,
    length: arr.len)

proc emptyBuffer[T](): WrappedBuffer[T] =
  WrappedBuffer[T](
    raw: nil,
    buf: nil,
    bytes: 0,
    length: 0)

proc declareTypeTagProc(dtype, name: NimNode): NimNode =
  let dataTypeNew = getDataTypeIdent(name)
  let getDataType = ident"getDataType"
  quote do:
    proc `getDataType`*(tag: TypeTag[`dtype`]): GArrowDataTypePtr =
      `dataTypeNew`()

macro DeclareNumericArray(dtype, name: untyped): untyped =
  let
    construct = ident"construct"
    getValue = ident"getValue"
    getValues = ident"getValues"
    glibArrayNew = arrayNewIdent(name)
    glibGetValue = arrayGetValueIdent(name)
    glibGetValues = arrayGetValuesIdent(name)

  result = newStmtList()
  result.add declareTypeTagProc(dtype, name)
  result.add quote do:
    proc `construct`(arr: var ArrowArray[`dtype`], length: int64,
                     offsets: GArrowBufferPtr, data: GArrowBufferPtr,
                     nullBitmap: GArrowBufferPtr, nNulls: int64) =
      arr.glibArray = `glibArrayNew`(length, data, nullBitmap, nNulls)

    proc `getValue`(arr: ArrowArray[`dtype`], i: int64): `dtype` =
      `glibGetValue`(arr.glibArray, i)

    proc `getValues`(arr: ArrowArray[`dtype`]): seq[`dtype`] =
      var valuesRead: int64
      let values = `glibGetValues`(arr.glibArray, valuesRead)

      for i in 0 .. valuesRead - 1:
        result.add values[i]

proc convertBytes[T](gbytes: GBytesPtr): T =
  var size: uint64
  let dataPtr = gbytesGetData(gbytes, size)
  when T is string:
    result = newString(size)
  elif T is Bytes:
    result = newSeq[byte](size)
  else:
    doAssert false

  if size > 0:
    copyMem(addr result[0], dataPtr, size)

macro DeclareBinaryArray(dtype, name: untyped): untyped =
  let
    construct = ident"construct"
    getValue = ident"getValue"
    getValues = ident"getValues"
    glibArrayNew = arrayNewIdent(name)

  result = newStmtList()
  result.add declareTypeTagProc(dtype, name)
  result.add quote do:
    proc `construct`(arr: var ArrowArray[`dtype`], length: int64,
                     offsets: GArrowBufferPtr, data: GArrowBufferPtr,
                     nullBitmap: GArrowBufferPtr, nNulls: int64) =
      arr.glibArray = `glibArrayNew`(length, offsets, data, nullBitmap, nNulls)

    proc `getValue`(arr: ArrowArray[`dtype`], i: int64): `dtype` =
      if not arrayIsNull(arr.glibArray, i):
        let gbytes = binaryArrayGetValue(arr.glibArray, i)
        result = convertBytes[`dtype`](gbytes)

    proc `getValues`(arr: ArrowArray[`dtype`]): seq[`dtype`] =
      for i in 0 .. arrayGetlength(arr.glibArray) - 1:
        if arrayIsNull(arr.glibArray, i):
          var empty: `dtype`
          result.add(empty)
        else:
          result.add(arr.getValue(i))

DeclareNumericArray(bool, boolean)
DeclareNumericArray(int8, int8)
DeclareNumericArray(uint8, uint8)
DeclareNumericArray(int16, int16)
DeclareNumericArray(uint16, uint16)
DeclareNumericArray(int32, int32)
DeclareNumericArray(uint32, uint32)
DeclareNumericArray(int64, int64)
DeclareNumericArray(uint64, uint64)
DeclareNumericArray(float32, float)
DeclareNumericArray(float64, double)

# TODO: handle dates
# DeclareNumericArray(int32, date32)
# DeclareNumericArray(int64, date64)

DeclareBinaryArray(string, string)
DeclareBinaryArray(Bytes, binary)

proc newArrowArray[T](offsets: WrappedBuffer[uint32], data: WrappedBuffer[T],
                      nullBitmap: WrappedBuffer[NullBitmapBase],
                      nNulls: int64): ArrowArray[T] =
  if offsets != nil:
    offsets.setGlibBuffer()
  data.setGlibBuffer()
  nullBitmap.setGlibBuffer()

  let length = when isBinary(T):
    offsets.length - 1
  else:
    data.length

  result = new(ArrowArray[T])
  result.offsets = offsets
  result.data = data
  result.nullBitmap = nullBitmap
  construct[T](result, length, offsets.buf, data.buf, nullBitmap.buf, nNulls)

proc newEmptyArrowArray*[T](): ArrowArray[T] =
  ## Constructs a new empty arrow array of type T.
  newArrowArray[T](emptyBuffer[uint32](), emptyBuffer[T](),
                   emptyBuffer[NullBitmapBase](), 0)

proc newArrowArray*[T](data: openArray[T]): ArrowArray[T] =
  ## Constructs a new arrow array of type T filled with `data`. Note, this
  ## creates a copy of `data` into a new internal buffer. For non-copying
  ## array construction, use an ArrowArrayBuilder[T].
  let builder = newArrowArrayBuilder[T]()
  for x in data:
    builder.add x

  builder.build()

proc newArrowArray*[T](data: openArray[Option[T]]): ArrowArray[T] =
  ## Constructs a new arrow array of type T filled with `data`. Treats
  ## `none(T)` as null. Note, this creates a copy of `data` into a new
  ## internal buffer. For non-copying array construction, use
  ## an ArrowArrayBuilder[T].
  let builder = newArrowArrayBuilder[T]()
  for x in data:
    builder.add x

  builder.build()

proc len*[T](arr: ArrowArray[T]): int64 =
  ## Returns the length of the arrow array.
  arrayGetLength(arr.glibArray)

proc isNullAt*[T](arr: ArrowArray[T], i: int64): bool =
  ## Returns true when the ith element of the array is null.
  arrayIsNull(arr.glibArray, i)

proc `@`*[T](arr: ArrowArray[T]): seq[T] =
  ## Converts the arrow array into a seq[T] (creates a copy).
  arr.getValues()

proc `$`*[T](arr: ArrowArray[T]): string =
  ## Returns the string representation of the array.
  var err: GErrorPtr
  let arrString = arrayToString(arr.glibArray, err)
  if err != nil:
    result = $err.message
    gErrorFree(err)
  else:
    result = $arrString
    gfree(arrString)

proc `[]`*[T](arr: ArrowArray[T], i: int64): T =
  ## Gets the ith element of the array. Note that null values will
  ## be returned as 0, so `isNullAt` should be checked first if
  ## the array may have null values.
  arr.getValue(i)

proc `[]`*[T](arr: ArrowArray[T], i: int): T =
  ## Gets the ith element of the array. Note that null values will
  ## be returned as 0, so `isNullAt` should be checked first if
  ## the array may have null values.
  arr[int64(i)]

proc `[]`*[T](arr: ArrowArray[T], slice: Slice[int]): ArrowArray[T] =
  ## Returns a slice of this array for the given range.
  arr[int64(slice.a) .. int64(slice.b)]

proc `[]`*[T](arr: ArrowArray[T], slice: Slice[int64]): ArrowArray[T] =
  ## Returns a slice of this array for the given range.
  let length = arr.len
  doAssert(slice.a >= 0 and slice.a < length and slice.b >= 0 and
           slice.b <= length and slice.a <= slice.b)

  let sliceLength = slice.b - slice.a
  let slice = arraySlice(arr.glibArray, slice.a, sliceLength)

  ArrowArray[T](glibArray: slice)

proc `=destroy`*[T](builder: var ArrowArrayBuilderObj[T]) =
  if builder.data != nil:
    dealloc(builder.data)

proc newArrowArrayBuilder*[T](): ArrowArrayBuilder[T] =
  ## Construct a new empty array builder.
  ArrowArrayBuilder[T](
    offsets: emptyBuffer[uint32](),
    data: emptyBuffer[T](),
    nullBitmap: newBitVector[NullBitmapBase](0),
    nNulls: 0,
    valid: true)

proc addNumeric[T](builder: ArrowArrayBuilder[T], x: T) =
  doAssert(builder.valid)
  builder.data.addElement(x)

proc addBinary[T](builder: ArrowArrayBuilder[T], x: T) =
  let currentOffset = builder.data.length
  if x.len > 0:
    builder.data.addBytes(unsafeAddr x[0], x.len)

  builder.offsets.addElement uint32(currentOffset)

proc add*[T](builder: ArrowArrayBuilder[T], x: T) =
  ## Add the element to the array.
  when isBinary(T):
    builder.addBinary(x)
  else:
    builder.addNumeric(x)

  builder.nullBitmap.add 1

proc add*[T](builder: ArrowArrayBuilder[T], x: Option[T]) =
  ## Add a value or null to the array, none(T) is treated as null.
  if x.isSome:
    when isBinary(T):
      builder.addBinary(x.get)
    else:
      builder.addNumeric(x.get)

    builder.nullBitmap.add 1

  else:
    when isBinary(T):
      var empty: T
      builder.addBinary(empty)
    else:
      builder.addNumeric(0)

    builder.nullBitmap.add 0
    builder.nNulls += 1

proc build*[T](builder: ArrowArrayBuilder[T]): ArrowArray[T] =
  ## Construct an arrow array from the builder's buffer. This does NOT
  ## create a copy of the data, and instead transfers ownership of the
  ## internal buffer to the array. After this is called, the builder
  ## is no longer valid and cannto be mutated.
  when isBinary(T):
    let currentOffset = builder.data.length
    builder.offsets.addElement uint32(currentOffset)

  let nullBitmapBuf = copyToBuffer[NullBitmapBase](builder.nullBitmap.base)
  result = newArrowArray[T](builder.offsets, builder.data,
                            nullBitmapBuf, builder.nNulls)
  builder.valid = false
  builder.data = nil