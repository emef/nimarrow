import std/options

import nimarrow_glib

import ./arrays
import ./tables

type
  ParquetWriterPropsObj = object
    glibProps: GParquetWriterPropertiesPtr
  ParquetWriterProps* = ref ParquetWriterPropsObj

  ParquetWriterObj = object
    closed: bool
    glibWriter: GParquetArrowFileWriterPtr
  ParquetWriter* = ref ParquetWriterObj

  ParquetReaderObj = object
    schema: ArrowSchema
    glibReader: GParquetArrowFileReaderPtr
  ParquetReader* = ref ParquetReaderObj

  TypedParquetWriter*[T] = ref object
    writer: ParquetWriter
    tableBuilder: TypedBuilder[T]

proc close*(w: ParquetWriter)

proc `=destroy`*(x: var ParquetWriterPropsObj) =
  if x.glibProps != nil:
    gObjectUnref(x.glibProps)

proc `=destroy`*(x: var ParquetWriterObj) =
  if x.glibWriter != nil:
    gObjectUnref(x.glibWriter)

proc `=destroy`*(x: var ParquetReaderObj) =
  if x.glibReader != nil:
    gObjectUnref(x.glibReader)

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

proc add*(w: ParquetWriter, table: ArrowTable) =
  ## Add this table to the parquet file being written.
  doAssert not w.closed

  var error: GErrorPtr
  let chunkSize = 1024'u64
  let success = parquetFileWriterWriteTable(
      w.glibWriter, table.glibPtr(), chunkSize, error)

  if error != nil:
    defer: gErrorFree(error)
    raise newException(IOError, $error.message)

  if not success:
    raise newException(IOError, "Error adding table to parquet writer")

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
  writer.add(t)
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

template newTypedParquetWriterTmpl(
    T: typedesc[TypeRegistered],
    path: string,
    props: Option[ParquetWriterProps] = none(ParquetWriterProps)
): TypedParquetWriter[T] =
  block:
    let typedWriter = new(TypedParquetWriter[T])
    typedWriter.tableBuilder = newTypedBuilder(T)
    typedWriter.writer = newParquetWriter(
      typedWriter.tableBuilder.schema, path, props)
    typedWriter

proc newTypedParquetWriter*[T: TypeRegistered](
    path: string,
    props: Option[ParquetWriterProps] = none(ParquetWriterProps)
): TypedParquetWriter[T] =
  ## Create a new typed parquet writer, writing to local path `path`.
  newTypedParquetWriterTmpl(T, path, props)

proc add*[T](w: TypedParquetWriter[T], x: T) =
  ## Append an element to the parquet file being written.
  w.tableBuilder.add x

proc close*[T](w: TypedParquetWriter[T]) =
  ## Close the parquet file for writing. NOTE: this MUST be called when
  ## done writing or the file will not be valid! This does not simply
  ## close the file descriptor, it finalizes the file by writing the parquet
  ## footer/metadata.
  w.writer.add w.tableBuilder.build
  w.writer.close

proc newParquetReader*(path: string, useThreads: bool = true): ParquetReader =
  ## Create a new parquet reader, reading the local path `path`.
  var err: GErrorPtr
  let glibReader = parquetFileReaderNewPath(path, err)
  if err != nil:
    defer: gErrorFree(err)
    raise newException(IOError, $err.message)

  parquetFileReaderSetUseThreads(glibReader, useThreads)

  let glibSchema = parquetFileReaderGetSchema(glibReader, err)
  if err != nil:
    defer: gErrorFree(err)
    raise newException(IOError, $err.message)

  let schema = newArrowSchema(glibSchema)

  ParquetReader(schema: schema, glibReader: glibReader)

proc rowGroups*(r: ParquetReader): int =
  ## Return the number of row groups in the file being read.
  parquetFileReaderGetNRowGroups(r.glibReader)

proc read*(r: ParquetReader, rowGroup: int): ArrowTable =
  ## Read the row group at index `rowGroup` as an ArrowTable.
  var err: GErrorPtr
  let glibTable = parquetFileReaderReadRowGroup(
    r.glibReader, rowGroup, nil, 0, err)

  if err != nil:
    defer: gErrorFree(err)
    raise newException(IOError, $err.message)

  newArrowTable(r.schema, glibTable)

proc readFully*(r: ParquetReader): ArrowTable =
  ## Read the entire parquet file into an ArrowTable.
  var err: GErrorPtr
  let glibTable = parquetFileReaderReadTable(r.glibReader, err)
  if err != nil:
    defer: gErrorFree(err)
    raise newException(IOError, $err.message)

  newArrowTable(r.schema, glibTable)

iterator iter*(r: ParquetReader, T: typedesc): T {.inline.} =
  ## Iterate over the file, converting the rows into the custom type `T`.
  # TODO: check schema
  let n = r.rowGroups
  for i in 0 ..< n:
    let grp = r.read(i)
    for x in grp.iter(T):
      yield x