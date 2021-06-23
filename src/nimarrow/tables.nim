import nimarrow_glib

import ./arrays

type
  ArrowFieldObj = object
    glibField: GArrowFieldPtr
  ArrowField* = ref ArrowFieldObj

  ArrowSchemaObj = object
    glibSchema: GArrowSchemaPtr
  ArrowSchema* = ref ArrowSchemaObj

  ArrowTableObj = object
    glibTable: GArrowTablePtr
  ArrowTable* = ref ArrowTableObj

  ArrowTableBuilder* = ref object
    schema: ArrowSchema
    glibArrays: seq[GArrowArrayPtr]

proc `=destroy`*(x: var ArrowFieldObj) =
  if x.glibField != nil:
    gObjectUnref(x.glibField)

proc `=destroy`*(x: var ArrowSchemaObj) =
  if x.glibSchema != nil:
    gObjectUnref(x.glibSchema)

proc `=destroy`*(x: var ArrowTableObj) =
  if x.glibTable != nil:
    gObjectUnref(x.glibTable)

proc newArrowField*[T](name: string, typeTag: TypeTag[T]): ArrowField =
  let glibDataType = getDataType(typeTag)
  result = ArrowField(glibField: fieldNew(name, glibDataType))
  gObjectUnref(glibDataType)

proc newArrowSchema*(fields: openArray[ArrowField]): ArrowSchema =
  var fieldList: GListPtr

  for field in fields:
    fieldList = glistAppend(fieldList, field.glibField)

  result = ArrowSchema(glibSchema: schemaNew(fieldList))
  glistFree(fieldList)

proc newArrowTableBuilder*(schema: ArrowSchema): ArrowTableBuilder =
  ArrowTableBuilder(schema: schema)

proc add*[T](b: ArrowTableBuilder, arr: ArrowArray[T]) =
  let i = b.glibArrays.len
  let expectedField = schemaGetField(b.schema.glibSchema, uint(i))
  let expectedDtype = fieldGetDataType(expectedField)
  doAssert dataTypeEqual(expectedDtype, getDataType(TypeTag[T]()))

  b.glibArrays.add arr.glibArray

proc build*(b: ArrowTableBuilder): ArrowTable =
  let glibArraysPtr = cast[ptr UncheckedArray[GArrowArrayPtr]](addr b.glibArrays[0])
  let nArrays = uint64(b.glibArrays.len)
  var error: GErrorPtr

  ArrowTable(
    glibTable: tableNewArrays(b.schema.glibSchema, glibArraysPtr, nArrays, error)
  )

proc len*(table: ArrowTable): uint64 =
  tableGetNRows(table.glibTable)

proc `$`*(table: ArrowTable): string =
  var error: GErrorPtr
  $tableToString(table.glibTable, error)