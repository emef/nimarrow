import nimarrow_glib

import ./arrays

## An ArrowTable is an ordered collection of named arrays (columns).
## Each column name and type is described by its ArrowField,
## and an ArrowSchema describes all of the columns in a table.
##
## To construct a table, we use an ArrowTableBuilder which is
## constructed with the intended schema. Each column's data must
## then be added to the builder in the order specified by the
## schema. Creating a table does not copy any of the column
## data, it will share the internal buffers of the arrays used
## to construct it.
runnableExamples:
  import nimarrow

  # Schema will be (a: int32, b: string)
  let field1 = newArrowField("a", TypeTag[int32]())
  let field2 = newArrowField("b", TypeTag[string]())
  let schema = newArrowSchema(@[field1, field2])

  # Column data for the described fields in the schema.
  let data1 = newArrowArray(@[1'i32, 2'i32, 3'i32])
  let data2 = newArrowArray(@["first", "second", "third"])

  # Add each column to the table in order specified by the schema.
  let tableBuilder = newArrowTableBuilder(schema)
  tableBuilder.add data1
  tableBuilder.add data2
  let table = tableBuilder.build

  # Convert the table into string representation including
  # it's metadata and all contents.
  discard $table

type
  ArrowFieldObj = object
    glibField: GArrowFieldPtr
  ArrowField* = ref ArrowFieldObj

  ArrowSchemaObj = object
    glibSchema: GArrowSchemaPtr
  ArrowSchema* = ref ArrowSchemaObj

  ArrowTableObj = object
    schema: ArrowSchema
    glibTable: GArrowTablePtr
  ArrowTable* = ref ArrowTableObj

  ArrowTableBuilder* = ref object
    valid: bool
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
  ## Create a new field of type T named `name`.
  let glibDataType = getDataType(typeTag)
  result = ArrowField(glibField: fieldNew(name, glibDataType))
  gObjectUnref(glibDataType)

proc glibPtr*(field: ArrowField): GArrowFieldPtr =
  ## Access the underlying glib field pointer.
  field.glibField

proc newArrowSchema*(fields: openArray[ArrowField]): ArrowSchema =
  ## Construct a new schema from a sequence of fields.
  var fieldList: GListPtr

  for field in fields:
    fieldList = glistAppend(fieldList, field.glibField)

  result = ArrowSchema(glibSchema: schemaNew(fieldList))
  glistFree(fieldList)

proc glibPtr*(schema: ArrowSchema): GArrowSchemaPtr =
  ## Access the underlying glib schema pointer.
  schema.glibSchema

proc newArrowTableBuilder*(schema: ArrowSchema): ArrowTableBuilder =
  ## Construct a new table builder for a given schema. Each column
  ## specified in the schema must be added using `add` in order.
  ArrowTableBuilder(schema: schema, valid: true)

proc add*[T](b: ArrowTableBuilder, arr: ArrowArray[T]) =
  ## Add the next column to the builder, must be of the correct type
  ## specified in the schema.
  doAssert b.valid

  let i = b.glibArrays.len
  let expectedField = schemaGetField(b.schema.glibSchema, uint(i))
  let expectedDtype = fieldGetDataType(expectedField)
  doAssert dataTypeEqual(expectedDtype, getDataType(TypeTag[T]()))

  b.glibArrays.add arr.glibPtr

proc build*(b: ArrowTableBuilder): ArrowTable =
  ## Build the table, invalidating this builder.
  doAssert uint(b.glibArrays.len) == schemaNFields(b.schema.glibPtr)
  b.valid = false

  let glibArraysPtr = cast[ptr UncheckedArray[GArrowArrayPtr]](addr b.glibArrays[0])
  let nArrays = uint64(b.glibArrays.len)
  var error: GErrorPtr

  ArrowTable(
    schema: b.schema,
    glibTable: tableNewArrays(b.schema.glibSchema, glibArraysPtr, nArrays, error)
  )

proc glibPtr*(table: ArrowTable): GArrowTablePtr =
  ## Access the underlying glib table pointer.
  table.glibTable

proc len*(table: ArrowTable): uint64 =
  ## Get the length (number of rows) of the table.
  tableGetNRows(table.glibTable)

proc `$`*(table: ArrowTable): string =
  ## String representation of the table's schema and full contents.
  var error: GErrorPtr
  $tableToString(table.glibTable, error)

proc schema*(table: ArrowTable): ArrowSchema =
  table.schema
