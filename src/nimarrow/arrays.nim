import std/macros
import std/options

import nimarrow_glib

import ./bitarray

type
  ArrowArrayObj[T] = object
    offsets: WrappedBuffer[uint32]
    data: WrappedBuffer[T]
    nullBitmap: WrappedBuffer[NullBitmapBase]
    glibArray: GArrowArrayPtr

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

  ArrowChunkedArrayObj[T] = object
    glibChunkedArray: GArrowChunkedArrayPtr

  ArrowChunkedArray*[T] = ref ArrowChunkedArrayObj[T]

  Bytes* = seq[byte] ## Binary type

  TypeTag*[T] = object  ## Empty container used to map generic type T into
                        ## the appropriate glib arrow data type internally.

proc `=destroy`*[T](x: var ArrowArrayObj[T]) =
  if x.glibArray != nil:
    gObjectUnref(x.glibArray)

proc `=destroy`*[T](x: var ArrowChunkedArrayObj[T]) =
  if x.glibChunkedArray != nil:
    gObjectUnref(x.glibChunkedArray)

proc `=destroy`*[T](x: var WrappedBufferObj[T]) =
  if x.raw != nil:
    dealloc(x.raw)

  if x.buf != nil:
    gObjectUnref(x.buf)

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
    iter = ident"iter"
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


    iterator `iter`(arr: ArrowArray[`dtype`]): `dtype` {.inline.} =
      var valuesRead: int64
      let values = `glibGetValues`(arr.glibArray, valuesRead)

      for i in 0 .. valuesRead - 1:
        yield values[i]

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
    iter = ident"iter"
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

    iterator `iter`(arr: ArrowArray[`dtype`]): `dtype` {.inline.} =
      var empty: `dtype`
      let size = arrayGetlength(arr.glibArray)
      for i in 0 ..< size:
        if arrayIsNull(arr.glibArray, i):
          yield empty
        else:
          yield arr.getValue(i)

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

proc toSeq*[T](arr: ArrowArray[T]): seq[T] =
  ## Converts the arrow array into a seq[T] (creates a copy).
  result = newSeqOfCap[T](arr.len)
  for x in arr:
    result.add x

proc `@`*[T](arr: ArrowArray[T]): seq[T] =
  ## Converts the arrow array into a seq[T] (creates a copy).
  arr.toSeq

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

iterator items*[T](arr: ArrowArray[T]): T {.inline.} =
  ## Iterate over each element in the array.
  for x in arr.iter():
    yield x

proc glibPtr*[T](arr: ArrowArray[T]): GArrowArrayPtr =
  ## Access the underlying glib array pointer.
  arr.glibArray

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

proc newArrowChunkedArray*[T](
    glibChunkedArray: GArrowChunkedArrayPtr): ArrowChunkedArray[T] =
  ## Construct a new chunked array from a glib chunked array pointer.
  let dtype = chunkedArrayGetValueDataType(glibChunkedArray)
  defer: gObjectUnref(dtype)

  let expectedDtype = getDataType(TypeTag[T]())
  defer: gObjectUnref(expectedDtype)

  doAssert dataTypeEqual(dtype, expectedDtype)
  ArrowChunkedArray[T](glibChunkedArray: glibChunkedArray)

proc len*[T](chunkedArray: ArrowChunkedArray[T]): uint64 =
  ## Return the number of total elements in the array (across all chunks).
  chunkedArrayGetNRows(chunkedArray.glibChunkedArray)

proc `$`*[T](chunkedArray: ArrowChunkedArray[T]): string =
  ## String representation of the chunked array.
  var err: GErrorPtr
  let asString = chunkedArrayToString(chunkedArray.glibChunkedArray, err)
  if err != nil:
    defer: gErrorFree(err)
    raise newException(CatchableError, $err.message)
  else:
    defer: gfree(asString)
    $asString

proc toSeq*[T](chunkedArray: ArrowChunkedArray[T]): seq[T] =
  ## Converts the chunked array into a seq[T] (creates a copy).
  result = newSeq[T]()
  for i in 0'u ..< chunkedArray.chunks:
    result.add @(chunkedArray.chunk(i))

proc `@`*[T](chunkedArray: ArrowChunkedArray[T]): seq[T] =
  ## Converts the chunked array into a seq[T] (creates a copy).
  chunkedArray.toSeq

proc `[]`*[T](chunkedArray: ArrowChunkedArray[T], i: int64): T =
  ## Get the element in the logical array represented by the chunked
  ## array at index `i`.
  doAssert uint64(i) < chunkedArray.len

  # TODO: lookup table + binsearch?
  var c = 0'u
  var chunk: ArrowArray[T]
  var offset = 0'i64
  while true:
    chunk = chunkedArray.chunk(c)
    if offset + chunk.len > i:
      break

    offset += chunk.len
    c += 1

  chunk[i - offset]

proc `==`*[T](a, b: ArrowChunkedArray[T]): bool =
  ## Compare two chunked arrays for equality.
  chunkedArrayEqual(a.glibChunkedArray, b.glibChunkedArray)

proc chunks*[T](chunkedArray: ArrowChunkedArray[T]): uint =
  ## Return the number of chunks in the chunked array.
  chunkedArrayGetNChunks(chunkedArray.glibChunkedArray)

proc chunk*[T](chunkedArray: ArrowChunkedArray[T], i: uint): ArrowArray[T] =
  ## Access the chunk at index `i`.
  doAssert i < chunkedArray.chunks
  let glibArray = chunkedArrayGetChunk(chunkedArray.glibChunkedArray, i)
  doAssert glibArray != nil
  ArrowArray[T](glibArray: glibArray)

proc combine*[T](chunkedArray: ArrowChunkedArray[T]): ArrowArray[T] =
  ## Combine all of the chunks in the chunked array into a single array,
  ## note this creates a copy.
  doAssert chunkedArray.isCorrectType[T]
  var err: GErrorPtr
  let glibArray = chunkedArrayCombine(chunkedArray.glibChunkedArray, err)
  if err != nil:
    defer: gErrorFree(err)
    raise newException(CatchableError, $err.message)

  doAssert glibArray != nil

  ArrowArray[T](glibArray: glibArray)

iterator items*[T](chunkedArray: ArrowChunkedArray[T]): T {.inline.} =
  ## Iterate over the all of the elements in the logical array represented
  ## by the chunked array.
  let chunks = chunkedArray.chunks
  for i in 0'u ..< chunks:
    let chunk = chunkedArray.chunk(i)
    for x in chunk:
      yield x
