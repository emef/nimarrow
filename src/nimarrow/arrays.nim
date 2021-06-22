import macros
import options

import nimarrow_glib

import ./bitarray

type
  ## A 1d array of type T. Data is owned by the object and stored 
  ## on heap, to be freed when this object is destroyed. In the 
  ## case this array is a slice of another array, it does not
  ## own its data and will not free any data on destruction.
  ArrowArray*[T] = object 
    data: WrappedBufferPtr  ## Buffer of the underlying memory backing this 
                            ## array. If nil, then this array is a slice of
                            ## another array and no data is owned.
    nullBitmap: WrappedBufferPtr # Buffer of the null bitmap, can be nil.
    glibArray: GArrowArrayPtr  ## g_object holding the arrow array
                               ## metadata. Freed on destruction.
  
  NullBitmapBase = uint32
  NullBitmap* = BitVector[NullBitmapBase]

  WrappedBuffer = object 
    raw: pointer
    buf: GArrowBufferPtr
    bytes: int64
    length: int64

  WrappedBufferPtr = ref WrappedBuffer

proc `=destroy`*[T](x: var ArrowArray[T]) =    
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
  result.data = data
  result.nullBitmap = nullBitmap
  construct[T](result, data.length, data.buf, nullBitmap.buf, nNulls)

proc newEmptyArrowArray*[T](): ArrowArray[T] =
  newArrowArray[T](emptyBuffer(), emptyBuffer(), 0)

proc newArrowArray*[T](data: openArray[T]): ArrowArray[T] =
  let data = copyToBuffer(data)  
  newArrowArray[T](data, emptyBuffer(), 0)

proc newArrowArray*[T](data: openArray[Option[T]]): ArrowArray[T]  

proc len*[T](arr: ArrowArray[T]): int64 =
  arrayGetLength(arr.glibArray)

proc isNullAt*[T](arr: ArrowArray[T], i: int64): bool =
  arrayIsNull(arr.glibArray, i)  

proc `@`*[T](arr: ArrowArray[T]): seq[T] =
  let length = arr.len
  var valuesRead: int64
  let values = arr.getValues(valuesRead)
  doAssert(valuesRead == length)

  for i in 0 .. valuesRead - 1:
    result.add values[i]

proc `$`*[T](arr: ArrowArray[T]): string =
  var err: GErrorPtr
  let arrString = arrayToString(arr.glibArray, err)    
  if err != nil:
    result = $err.message
    gErrorFree(err)
  else:
    result = $arrString
    gfree(arrString)

proc `[]`*[T](arr: ArrowArray[T], i: int64): T =   
  arr.getValue(i)

proc `[]`*[T](arr: ArrowArray[T], bounds: Slice[int]): ArrowArray[T] =
  arr[int64(bounds.a) .. int64(bounds.b)]

proc `[]`*[T](arr: ArrowArray[T], bounds: Slice[int64]): ArrowArray[T] =
  let length = arr.len
  doAssert(bounds.a >= 0 and bounds.a < length and bounds.b >= 0 and 
           bounds.b < length and bounds.a <= bounds.b)
  
  let sliceLength = bounds.b - bounds.a  
  let slice = arraySlice(arr.glibArray, bounds.a, sliceLength)

  ArrowArray(
    glibArray: slice,
    getValue: arr.getValue,
    getValues: arr.getValues
  )

type
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
  ArrowArrayBuilder[T](
    data: nil,
    nullBitmap: newBitVector[NullBitmapBase](0),
    length: 0,
    cap: 0,
    nNulls: 0,
    valid: true
  )

proc reserve*[T](builder: ArrowArrayBuilder[T], maxSize: int64) =
  assert(builder.valid)
  
  let minSize = int64(64 / sizeof(T))
  let newBytes = max(minSize, maxSize) * sizeof(T)
  if builder.data == nil:
    builder.data = cast[ptr UncheckedArray[T]](alloc(newBytes))
  elif maxSize > builder.cap:
    builder.data = cast[ptr UncheckedArray[T]](realloc(builder.data, newBytes))

  builder.cap = maxSize

proc ensureCap[T](builder: ArrowArrayBuilder[T]) =
  if builder.length == builder.cap:
    builder.reserve(min(2, builder.cap * 2))

proc add*[T](builder: ArrowArrayBuilder[T], x: T) =
  assert(builder.valid)
  ensureCap(builder)
  builder.data[builder.length] = x
  builder.nullBitmap.add 1
  builder.length += 1

proc add*[T](builder: ArrowArrayBuilder[T], x: Option[T]) =
  assert(builder.valid)
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
  let bytes = builder.cap * sizeof(T)  
  let buf = WrappedBufferPtr(
    raw: builder.data,
    buf: bufferNew(builder.data, bytes),
    bytes: bytes,
    length: builder.length
  )
  let nullBitmapBuf = copyToBuffer(builder.nullBitmap.base)
  result = newArrowArray[T](buf, nullBitmapBuf, builder.nNulls)
  builder.valid = false
  builder.data = nil

proc newArrowArray*[T](data: openArray[Option[T]]): ArrowArray[T] =
  let builder = newArrowArrayBuilder[T]()
  for x in data:
    builder.add x

  builder.build()