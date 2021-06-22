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
  ArrowArrayInternal[T] = object 
    data: WrappedBufferPtr
    nullBitmap: WrappedBufferPtr
    glibArray: GArrowArrayPtr

  ArrowArray*[T] = ref ArrowArrayInternal[T]  
  
  NullBitmapBase = uint32
  NullBitmap* = BitVector[NullBitmapBase]

  WrappedBuffer = object 
    raw: pointer
    buf: GArrowBufferPtr
    bytes: int64
    length: int64

  WrappedBufferPtr = ref WrappedBuffer

proc `=destroy`*[T](x: var ArrowArrayInternal[T]) =    
  if x.glibArray != nil:
    gObjectUnref(x.glibArray)  
    
proc `=destroy`*(x: var WrappedBuffer) =
  if x.raw != nil:
    dealloc(x.raw)
    
  if x.buf != nil:
    gObjectUnref(x.buf)

proc copyToBuffer[T](arr: openArray[T]): WrappedBufferPtr =
  let bytes = ((sizeof(T) * arr.len + 64) / 64).toInt
  let raw = cast[ptr UncheckedArray[T]](alloc(bytes))
  for i, x in arr:
    raw[i] = x                    
  
  WrappedBufferPtr(
    raw: raw,
    buf: bufferNew(raw, bytes),
    bytes: bytes,
    length: arr.len)

proc emptyBuffer(): WrappedBufferPtr =  
  WrappedBufferPtr(
    raw: nil,
    buf: nil,
    bytes: 0,
    length: 0)

macro DeclareNumericArray(dtype, name: untyped): untyped =
  let 
    construct = ident"construct"
    getValue = ident"getValue"
    getValues = ident"getValues"
    glibArrayNew = arrayNewIdent(name)
    glibGetValue = arrayGetValueIdent(name)
    glibGetValues = arrayGetValuesIdent(name)    

  result = quote do:
    proc `construct`(arr: var ArrowArray[`dtype`], length: int64, data: GArrowBufferPtr, 
                     nullBitmap: GArrowBufferPtr, nNulls: int64) =
      arr.glibArray = `glibArrayNew`(length, data, nullBitmap, nNulls) 
    
    proc `getValue`(arr: ArrowArray[`dtype`], i: int64): `dtype` =
      `glibGetValue`(arr.glibArray, i)

    proc `getValues`(arr: ArrowArray[`dtype`], 
                     length: var int64): ptr UncheckedArray[`dtype`] =
      `glibGetValues`(arr.glibArray, length)

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

proc newArrowArray[T](data: WrappedBufferPtr, nullBitmap: WrappedBufferPtr, 
                      nNulls: int64): ArrowArray[T] =
  result = new(ArrowArray[T])                 
  result.data = data
  result.nullBitmap = nullBitmap
  construct[T](result, data.length, data.buf, nullBitmap.buf, nNulls)

proc newEmptyArrowArray*[T](): ArrowArray[T] =
  ## Constructs a new empty arrow array of type T.
  newArrowArray[T](emptyBuffer(), emptyBuffer(), 0)

proc newArrowArray*[T](data: openArray[T]): ArrowArray[T] =
  ## Constructs a new arrow array of type T filled with `data`. Note, this
  ## creates a copy of `data` into a new internal buffer. For non-copying
  ## array construction, use an ArrowArrayBuilder[T].
  let data = copyToBuffer(data)  
  newArrowArray[T](data, emptyBuffer(), 0)

proc newArrowArray*[T](data: openArray[Option[T]]): ArrowArray[T]  
  ## Constructs a new arrow array of type T filled with `data`. Treats
  ## `none(T)` as null. Note, this creates a copy of `data` into a new
  ## internal buffer. For non-copying array construction, use 
  ## an ArrowArrayBuilder[T].

proc len*[T](arr: ArrowArray[T]): int64 =
  ## Returns the length of the arrow array.
  arrayGetLength(arr.glibArray)

proc isNullAt*[T](arr: ArrowArray[T], i: int64): bool =
  ## Returns true when the ith element of the array is null.
  arrayIsNull(arr.glibArray, i)  

proc `@`*[T](arr: ArrowArray[T]): seq[T] =
  ## Converts the arrow array into a seq[T] (creates a copy).
  let length = arr.len
  var valuesRead: int64
  let values = arr.getValues(valuesRead)
  doAssert(valuesRead == length)

  for i in 0 .. valuesRead - 1:
    result.add values[i]

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

type
  ## Supports building an array by adding one element at a time.
  ArrowArrayBuilderObj[T] = object
    data: ptr UncheckedArray[T]
    nullBitmap: NullBitmap    
    length: int64
    cap: int64
    nNulls: int64
    valid: bool

  ArrowArrayBuilder*[T] = ref ArrowArrayBuilderObj[T]
    
proc `=destroy`*[T](builder: var ArrowArrayBuilderObj[T]) =
  if builder.data != nil:
    dealloc(builder.data)

proc newArrowArrayBuilder*[T](): ArrowArrayBuilder[T] =
  ## Construct a new empty array builder.    
  ArrowArrayBuilder[T](
    data: nil,
    nullBitmap: newBitVector[NullBitmapBase](0),
    length: 0,
    cap: 0,
    nNulls: 0,
    valid: true)

proc reserve*[T](builder: ArrowArrayBuilder[T], maxSize: int64) =
  ## Reserve `maxSize` elements in the array's buffer.    
  doAssert(builder.valid)
  
  let minSize = int64(64 / sizeof(T))
  let newBytes = max(minSize, maxSize) * sizeof(T)
  if builder.data == nil:
    builder.data = cast[ptr UncheckedArray[T]](alloc(newBytes))
  elif maxSize > builder.cap:
    builder.data = cast[ptr UncheckedArray[T]](realloc(builder.data, newBytes))

  builder.cap = maxSize

proc ensureCap[T](builder: ArrowArrayBuilder[T]) =
  doAssert(builder.valid)
  if builder.length == builder.cap:
    builder.reserve(min(2, builder.cap * 2))

proc add*[T](builder: ArrowArrayBuilder[T], x: T) =
  ## Add the element to the array.    
  doAssert(builder.valid)
  ensureCap(builder)
  builder.data[builder.length] = x
  builder.nullBitmap.add 1
  builder.length += 1

proc add*[T](builder: ArrowArrayBuilder[T], x: Option[T]) =
  ## Add a value or null to the array, none(T) is treated as null.    
  doAssert(builder.valid)
  ensureCap(builder)

  if x.isSome:
    builder.data[builder.length] = x.get
    builder.nullBitmap.add 1
  else:
    builder.data[builder.length] = 0
    builder.nullBitmap.add 0
    builder.nNulls += 1
  
  builder.length += 1  

proc build*[T](builder: ArrowArrayBuilder[T]): ArrowArray[T] =
  ## Construct an arrow array from the builder's buffer. This does NOT
  ## create a copy of the data, and instead transfers ownership of the
  ## internal buffer to the array. After this is called, the builder
  ## is no longer valid and cannto be mutated.    
  let bytes = builder.cap * sizeof(T)  
  let buf = WrappedBufferPtr(
    raw: builder.data,
    buf: bufferNew(builder.data, bytes),
    bytes: bytes,
    length: builder.length)
  let nullBitmapBuf = copyToBuffer(builder.nullBitmap.base)
  result = newArrowArray[T](buf, nullBitmapBuf, builder.nNulls)
  builder.valid = false
  builder.data = nil

proc newArrowArray*[T](data: openArray[Option[T]]): ArrowArray[T] =
  ## Construct an arrow array from elements which may be null, none(T)
  ## is treated as null.    
  let builder = newArrowArrayBuilder[T]()
  for x in data:
    builder.add x

  builder.build()