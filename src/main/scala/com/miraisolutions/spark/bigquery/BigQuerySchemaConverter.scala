package com.miraisolutions.spark.bigquery

import com.google.cloud.bigquery._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import java.sql.Timestamp
import java.time.Instant
import java.util

import LegacySQLTypeName._
import com.google.common.io.BaseEncoding
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

private object BigQuerySchemaConverter {

  private val logger = LoggerFactory.getLogger(this.getClass.getName)

  private val KEY_FIELD_NAME = "key"
  private val VALUE_FIELD_NAME = "value"

  def fromBigQueryToSpark(schema: Schema): StructType = {
    logger.info(s"Schema: $schema")
    logger.info(s"Fields: ${schema.getFields}")
    val fields = schema.getFields.asScala.map(bigQueryToSparkField)
    StructType(fields)
  }

  def getBigQueryToSparkConverterFunction(schema: Schema): FieldValueList => Row = { fields =>
    val meta = fromBigQueryToSpark(schema).fields.zip(fields.asScala)
    val values = meta map { case (field, value) => getRowValue(value, field.dataType) }
    Row.fromSeq(values)
  }

  private def bigQueryToSparkField(field: Field): StructField = {
    val dataType = field.getType match {
      case BOOLEAN =>
        BooleanType

      case INTEGER =>
        LongType

      case FLOAT =>
        DoubleType

      case STRING =>
        StringType

      case BYTES =>
        BinaryType

      case RECORD =>
        val fields = field.getSubFields.asScala.map(bigQueryToSparkField)
        StructType(fields)

      case TIMESTAMP =>
        TimestampType

      case DATE =>
        StringType

      case TIME =>
        // Not supported in Spark
        StringType

      case DATETIME =>
        // Not supported in Spark
        StringType
    }

    val isNullable = field.getMode.equals(Field.Mode.NULLABLE)
    val isRepeated = field.getMode.equals(Field.Mode.REPEATED)

    if(isRepeated) {
      StructField(field.getName, ArrayType(dataType, isNullable), false)
    } else {
      StructField(field.getName, dataType, isNullable)
    }
  }

  private def getRowValue(value: FieldValue, dataType: DataType): Any = {
    if(value.isNull) {
      null
    } else {
      dataType match {
        case BooleanType =>
          value.getBooleanValue
        case LongType =>
          value.getLongValue
        case DoubleType =>
          value.getDoubleValue
        case StringType =>
          value.getStringValue
        case BinaryType =>
          value.getBytesValue
        case ArrayType(elementType, _) =>
          value.getRepeatedValue.asScala.map(getRowValue(_, elementType)).toArray
        case StructType(fields) =>
          value.getRecordValue.asScala.zip(fields.map(_.dataType)).map((getRowValue _).tupled).toArray
        case TimestampType =>
          Timestamp.from(Instant.ofEpochMilli(value.getTimestampValue / 1000))
        case DateType =>
          value.getStringValue
      }
    }
  }

  def fromSparkToBigQuery(schema: StructType): Schema = {
    Schema.of(schema.fields.map(sparkToBigQueryField): _*)
  }

  private def customKeyValueStructFields(mapType: MapType): (StructField, StructField) = {
    val keyField = StructField(KEY_FIELD_NAME, mapType.keyType, false)
    val valueField = StructField(VALUE_FIELD_NAME, mapType.valueType, mapType.valueContainsNull)
    (keyField, valueField)
  }

  private def customArrayStructField(arrayType: ArrayType): StructField = {
    StructField(VALUE_FIELD_NAME, arrayType.elementType, arrayType.containsNull)
  }

  private def sparkToBigQueryField(field: StructField): Field = {
    def f(tpe: LegacySQLTypeName): Field = {
      val mode = if(field.nullable) Field.Mode.NULLABLE else Field.Mode.REQUIRED
      Field.newBuilder(field.name, tpe).setMode(mode).build()
    }

    field.dataType match {
      case BooleanType =>
        f(BOOLEAN)

      case ByteType =>
        f(INTEGER)

      case ShortType =>
        f(INTEGER)

      case IntegerType =>
        f(INTEGER)

      case LongType =>
        f(INTEGER)

      case FloatType =>
        f(FLOAT)

      case DoubleType =>
        f(FLOAT)

      case _: DecimalType =>
        f(STRING)

      case StringType =>
        f(STRING)

      case BinaryType =>
        f(BYTES)

      case StructType(fields) =>
        Field.of(field.name, RECORD, fields.map(sparkToBigQueryField): _*)

      case TimestampType =>
        f(TIMESTAMP)

      case DateType =>
        f(STRING)

      case t: MapType =>
        val (keyField, valueField) = customKeyValueStructFields(t)
        val key = sparkToBigQueryField(keyField)
        val value = sparkToBigQueryField(valueField)
        Field.newBuilder(field.name, RECORD, key, value).setMode(Field.Mode.REPEATED).build()

      case t: ArrayType =>
        val elementField = sparkToBigQueryField(customArrayStructField(t))
        Field.newBuilder(field.name, elementField.getType).setMode(Field.Mode.REPEATED).build()

      case _ => // HiveStringType, NullType, ObjectType, CalendarIntervalType
        f(STRING)
        // TODO ???
    }
  }

  def getSparkToBigQueryConverterFunction(schema: StructType): Row => util.Map[String, Any]/*FieldValueList*/ = { row =>
    val meta = fromSparkToBigQuery(schema)
    val result = new util.HashMap[String, Any](meta.getFields.size)

    meta.getFields.asScala foreach { field =>
      result.put(field.getName, getFieldValue(row, field))
    }
    result
  }

  // Yes, String. See docs of FieldValue.of(...);
  // see also https://github.com/GoogleCloudPlatform/google-cloud-java/pull/2891
  // private def primitive(value: String): FieldValue = FieldValue.of(FieldValue.Attribute.PRIMITIVE, value)


  private def getFieldValue(row: Row, field: Field): Any /*FieldValue*/ = {
    val idx = row.fieldIndex(field.getName)

    if(row.isNullAt(idx)) {
      null
      // primitive(null)
    } else {
      val sourceType = row.schema(idx).dataType
      val targetType = field.getType

      (sourceType, targetType) match {
        case (BooleanType, BOOLEAN) =>
          row.getBoolean(idx)
          // primitive(row.getBoolean(idx).toString)

        case (ByteType, INTEGER) =>
          row.getByte(idx)
          // primitive(row.getByte(idx).toString)

        case (ShortType, INTEGER) =>
          row.getShort(idx)
          // primitive(row.getShort(idx).toString)

        case (IntegerType, INTEGER) =>
          row.getInt(idx)
          // primitive(row.getInt(idx).toString)

        case (LongType, INTEGER) =>
          row.getLong(idx)
          // primitive(row.getLong(idx).toString)

        case (FloatType, FLOAT) =>
          row.getFloat(idx)
          // primitive(row.getFloat(idx).toString)

        case (DoubleType, FLOAT) =>
          row.getDouble(idx)
          // primitive(row.getDouble(idx).toString)

        case (_: DecimalType, STRING) =>
          row.getDecimal(idx)
          // primitive(row.getDecimal(idx).toString)

        case (StringType, STRING) =>
          row.getString(idx)
          // primitive(row.getString(idx))

        case (BinaryType, BYTES) =>
          val bytes = row.getAs[Array[Byte]](idx)
          BaseEncoding.base64().encode(bytes)
          // primitive(BaseEncoding.base64().encode(bytes))

        case (StructType(_), RECORD) =>
          val struct = row.getStruct(idx)
          val result = new util.HashMap[String, Any](struct.size)

          field.getSubFields.asScala foreach { subField =>
            result.put(subField.getName, getFieldValue(struct, subField))
          }
          result

          /*val fields = field.getSubFields.asScala map { field =>
            getFieldValue(row.getStruct(idx), field)
          }
          val fieldValueList = FieldValueList.of(fields.asJava, field.getSubFields)
          FieldValue.of(FieldValue.Attribute.RECORD, fieldValueList)*/

        case (TimestampType, TIMESTAMP) =>
          row.getTimestamp(idx).getTime
          // primitive(row.getTimestamp(idx).getTime.toString)

        case (DateType, STRING) =>
          row.getDate(idx).toString
          // primitive(row.getDate(idx).toString)

        case (t: MapType, RECORD) =>
          val m = row.getMap[Any, Any](idx)
          val result = new util.HashMap[Any, Any](m.size)

          val (keyField, valueField) = customKeyValueStructFields(t)
          val mapSchema = StructType(Array(keyField, valueField))

          m foreach { kv =>
            val kvRow = new GenericRowWithSchema(kv.productIterator.toArray, mapSchema)
            val keyValue = getFieldValue(kvRow, field.getSubFields.get(0))
            val valueValue = getFieldValue(kvRow, field.getSubFields.get(1))
            result.put(keyValue, valueValue)
          }
          result

          /*val (keyField, valueField) = customKeyValueStructFields(t)
          val mapSchema = StructType(Array(keyField, valueField))

          val mapFields = row.getMap(idx) map { kv =>
            val kvRow = new GenericRowWithSchema(kv.productIterator.toArray, mapSchema)
            val kvFields = field.getSubFields.asScala map { field =>
              getFieldValue(kvRow, field)
            }
            val kvFieldValueList = FieldValueList.of(kvFields.asJava, field.getSubFields)
            FieldValue.of(FieldValue.Attribute.RECORD, kvFieldValueList)
          }
          val mapFieldValueList = FieldValueList.of(mapFields.toList.asJava, field.getSubFields)
          FieldValue.of(FieldValue.Attribute.REPEATED, mapFieldValueList)*/

        case (st: ArrayType, _) =>
          val arraySchema = StructType(Array(customArrayStructField(st)))
          row.getSeq[Any](idx) map { value =>
            getFieldValue(new GenericRowWithSchema(Array(value), arraySchema), field)
          } asJava


          /*val arraySchema = StructType(Array(customArrayStructField(t)))
          val arrayFields = row.getSeq[Any](idx) map { value =>
            getFieldValue(new GenericRowWithSchema(Array(value), arraySchema), field)
          }
          val arrayFieldValueList = FieldValueList.of(arrayFields.asJava, FieldList.of(field))
          FieldValue.of(FieldValue.Attribute.REPEATED, arrayFieldValueList)*/
      }
    }
  }
}
