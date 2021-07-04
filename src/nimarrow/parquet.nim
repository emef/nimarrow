import options

import nimarrow_glib

import ./tables

runnableExamples:
  import nimarrow
  let field1 = newArrowField("a", TypeTag[int32]())
  let field2 = newArrowField("b", TypeTag[string]())

  let data1 = newArrowArray(@[1'i32, 2'i32, 3'i32])
  let data2 = newArrowArray(@["first", "second", "third"])

  let schema = newArrowSchema(@[field1, field2])

  let tableBuilder = newArrowTableBuilder(schema)
  tableBuilder.add data1
  tableBuilder.add data2
  let table = tableBuilder.build

  table.toParquet("/tmp/test.parquet")

  let rereadTable = fromParquet("/tmp/test.parquet")
  echo $rereadTable

type
  ParquetWriterPropsObj = object
    glibProps: GParquetWriterPropertiesPtr
  ParquetWriterProps* = ref ParquetWriterPropsObj

  ParquetWriterObj = object
    closed: bool
    glibWriter: GParquetArrowFileWriterPtr
  ParquetWriter* = ref ParquetWriterObj

proc close*(w: ParquetWriter)

proc `=destroy`*(x: var ParquetWriterPropsObj) =
  if x.glibProps != nil:
    gObjectUnref(x.glibProps)

proc `=destroy`*(x: var ParquetWriterObj) =
  if x.glibWriter != nil:
    gObjectUnref(x.glibWriter)

proc newParquetWriterProps*(
    compression: GArrowCompressionType = GARROW_COMPRESSION_TYPE_SNAPPY,
    enableDictionary: bool = true,
    dictionaryPageSizeLimit: Option[int64] = none(int64),
    batchSize: Option[int64] = none(int64),
    maxRowGroupLength: Option[int64] = none(int64),
    dataPageSize: Option[int64] = none(int64)
): ParquetWriterProps =
  ## Construct a new parquet writer properties object, optionally overriding
  ## the default settings.
  let props = writerPropertiesNew()
  props.writerPropertiesSetCompression(compression, nil)

  if enableDictionary:
    props.writerPropertiesEnableDictionary(nil)
  else:
    props.writerPropertiesDisableDictionary(nil)

  if batchSize.isSome:
    props.writerPropertiesSetBatchSize(batchSize.get)

  if maxRowGroupLength.isSome:
    props.writerPropertiesSetMaxRowGroupLength(maxRowGroupLength.get)

  if dataPageSize.isSome:
    props.writerPropertiesSetDataPageSize(dataPageSize.get)

  ParquetWriterProps(glibProps: props)

proc newParquetWriter*(
    schema: ArrowSchema,
    path: string,
    props: Option[ParquetWriterProps] = none(ParquetWriterProps)
): ParquetWriter =
  ## Construct a new parquet writer which will write to the local file
  ## at `path`.
  var error: GErrorPtr

  let actualProps = if props.isSome:
    props.get
  else:
    newParquetWriterProps()

  let writer = parquetFileWriterNewPath(
    schema.glibPtr(),
    path,
    actualProps.glibProps,
    error
  )

  if error != nil:
    defer: gErrorFree(error)
    raise newException(IOError, $error.message)

  ParquetWriter(glibWriter: writer)

proc append*(w: ParquetWriter, table: ArrowTable) =
  ## Append this table to the parquet file being written.
  doAssert not w.closed

  var error: GErrorPtr
  let chunkSize = 1024'u64
  let success = parquetFileWriterWriteTable(
      w.glibWriter, table.glibPtr(), chunkSize, error)

  if error != nil:
    defer: gErrorFree(error)
    raise newException(IOError, $error.message)

  if not success:
    raise newException(IOError, "Error appending table to parquet writer")

proc close*(w: ParquetWriter) =
  ## Close the parquet file for writing. NOTE: this MUST be called when
  ## done writing or the file will not be valid! This does not simply
  ## close the file descriptor, it finalizes the file by writing the parquet
  ## footer/metadata.
  var error: GErrorPtr
  let success = parquetFileWriterClose(w.glibWriter, error)
  if error != nil:
    defer: gErrorFree(error)
    raise newException(IOError, $error.message)

  if not success:
    raise newException(IOError, "Error closing parquet writer")

  w.closed = true

proc toParquet*(
    t: ArrowTable,
    path: string,
    props: Option[ParquetWriterProps] = none(ParquetWriterProps)
) =
  ## Write this table to a parquet file on the local filesystem at `path`.
  let writer = newParquetWriter(t.schema, path, props)
  writer.append(t)
  writer.close()

proc fromParquet*(path: string): ArrowTable =
  ## Read a parquet file from the local filesystem at `path` into a Table.
  var error: GErrorPtr
  let reader = parquetFileReaderNewPath(path, error)
  if error != nil:
    defer: gErrorFree(error)
    raise newException(IOError, $error.message)

  defer: gObjectUnref(reader)

  let glibSchema = parquetFileReaderGetSchema(reader, error)
  if error != nil:
    defer: gErrorFree(error)
    raise newException(IOError, $error.message)

  let schema = newArrowSchema(glibSchema)

  let glibTable = parquetFileReaderReadTable(reader, error)
  if error != nil:
    defer: gErrorFree(error)
    raise newException(IOError, $error.message)

  newArrowTable(schema, glibTable)


