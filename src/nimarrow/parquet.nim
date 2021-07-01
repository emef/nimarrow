import options

import nimarrow_glib

import ./tables

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
  # TODO: I don't think these destroy methods are even being called...
  if not x.closed:
    # TODO: how to share this code with close() when the type are var T vs ref T?
    var error: GErrorPtr
    discard parquetFileWriterClose(x.glibWriter, error)
    x.closed = true

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

  ParquetWriter(glibWriter: writer)

proc append*(w: ParquetWriter, table: ArrowTable) =
  doAssert not w.closed

  var error: GErrorPtr
  let chunkSize = 1024'u64
  discard parquetFileWriterWriteTable(
      w.glibWriter, table.glibPtr(), chunkSize, error)

proc close*(w: ParquetWriter) =
  var error: GErrorPtr
  discard parquetFileWriterClose(w.glibWriter, error)
  w.closed = true

proc toParquet*(
    t: ArrowTable,
    path: string,
    props: Option[ParquetWriterProps] = none(ParquetWriterProps)
) =
  let writer = newParquetWriter(t.schema, path, props)
  writer.append(t)
  writer.close()
