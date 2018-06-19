/*
 * Copyright (c) 2018 Mirai Solutions GmbH
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.miraisolutions.spark.bigquery

import com.google.cloud.bigquery._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import LegacySQLTypeName._
import com.google.common.io.BaseEncoding
import com.miraisolutions.spark.bigquery.utils.DateTime
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

import scala.collection.JavaConverters._
import scala.language.postfixOps

/**
  * Schema conversion functions to convert schemas between Apache Spark and Google BigQuery.
  * @see https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types
  */
private[bigquery] object BigQuerySchemaConverter {

  // BigQuery's NUMERIC is a Decimal with precision 38 and scale 9;
  // See https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types
  private[bigquery] val BIGQUERY_NUMERIC_DECIMAL = DataTypes.createDecimalType(38, 9)

  private val KEY_FIELD_NAME = "key"
  private val VALUE_FIELD_NAME = "value"

  /**
    * Converts a BigQuery schema to a Spark schema.
    * @param schema BigQuery schema
    * @return Spark schema
    */
  def fromBigQueryToSpark(schema: Schema): StructType = {
    val fields = schema.getFields.asScala.map(bigQueryToSparkField)
    StructType(fields)
  }

  /**
    * Creates a function that can be used to convert a BigQuery row (represented as [[FieldValueList]]) to a Spark
    * row.
    * @param schema BigQuery schema
    * @return Function to convert a BigQuery row to a Spark row
    */
  def getBigQueryToSparkConverterFunction(schema: Schema): FieldValueList => Row = { fields =>
    val meta = fromBigQueryToSpark(schema).fields.zip(fields.asScala)
    val values = meta map { case (field, value) => getRowValue(value, field.dataType) }
    Row.fromSeq(values)
  }

  /**
    * Converts a BigQuery [[Field]] to a Spark [[StructField]].
    * @param field BigQuery [[Field]]
    * @return Spark [[StructField]]
    */
  private def bigQueryToSparkField(field: Field): StructField = {
    val dataType = field.getType match {
      case BOOLEAN =>
        BooleanType

      case INTEGER =>
        LongType

      case FLOAT =>
        DoubleType

      case NUMERIC =>
        BIGQUERY_NUMERIC_DECIMAL

      case STRING =>
        StringType

      case BYTES =>
        BinaryType

      case RECORD =>
        val fields = field.getSubFields.asScala.map(bigQueryToSparkField)
        StructType(fields.toArray)

      case TIMESTAMP =>
        TimestampType

      case DATE =>
        DateType

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

  /**
    * Extracts a value from a BigQuery field that can be used to construct a Spark row.
    * @param value BigQuery [[FieldValue]]
    * @param dataType Target Spark data type
    * @return Spark row value
    */
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
        case _: DecimalType =>
          value.getNumericValue
        case StringType =>
          value.getStringValue
        case BinaryType =>
          value.getBytesValue
        case ArrayType(elementType, _) =>
          value.getRepeatedValue.asScala.map(getRowValue(_, elementType)).toArray
        case StructType(fields) =>
          Row(value.getRecordValue.asScala.zip(fields.map(_.dataType)).map((getRowValue _).tupled): _*)
        case TimestampType =>
          DateTime.epochMicrosToTimestamp(value.getTimestampValue)
        case DateType =>
          DateTime.parseDate(value.getStringValue)
      }
    }
  }

  /**
    * Converts a Spark schema to a BigQuery schema.
    * @param schema Spark schema
    * @return BigQuery schema
    */
  def fromSparkToBigQuery(schema: StructType): Schema = {
    Schema.of(schema.fields.map(sparkToBigQueryField): _*)
  }

  /**
    * Creates a custom (key, value) [[StructField]] pair from a Spark [[MapType]] that can be used
    * to construct a BigQuery record type.
    * @param mapType Spark map type
    * @return Key/value [[StructField]] pair according to the map's key/value data types
    */
  private def customKeyValueStructFields(mapType: MapType): (StructField, StructField) = {
    val keyField = StructField(KEY_FIELD_NAME, mapType.keyType, false)
    val valueField = StructField(VALUE_FIELD_NAME, mapType.valueType, mapType.valueContainsNull)
    (keyField, valueField)
  }

  /**
    * Creates a custom [[StructField]] from a Spark [[ArrayType]] that can be used to construct a BigQuery
    * field with "repeated" mode.
    * @param arrayType Spark array type
    * @return [[StructField]] according to the array's element data type
    */
  private def customArrayStructField(arrayType: ArrayType): StructField = {
    StructField(VALUE_FIELD_NAME, arrayType.elementType, arrayType.containsNull)
  }

  /**
    * Converts a Spark [[StructField]] to a BigQuery [[Field]].
    * @param field Spark [[StructField]]
    * @return BigQuery [[Field]]
    */
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

      case dt: DecimalType if dt.precision <= BIGQUERY_NUMERIC_DECIMAL.precision &&
        dt.scale <= BIGQUERY_NUMERIC_DECIMAL.scale =>
        f(NUMERIC)

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
        f(DATE)

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
    }
  }

  /**
    * Creates a function that can be used to convert a Spark row to a BigQuery row (represented as a map).
    * @param schema Spark schema
    * @return Function to convert a Spark row to a BigQuery row
    */
  def getSparkToBigQueryConverterFunction(schema: StructType): Row => java.util.Map[String, Any] = { row =>
    val meta = fromSparkToBigQuery(schema)
    val result = new java.util.HashMap[String, Any](meta.getFields.size)

    meta.getFields.asScala foreach { field =>
      result.put(field.getName, getFieldValue(row, field))
    }
    result
  }

  /**
    * Extracts a value from a Spark row that can be used to construct a BigQuery row.
    * @param row Spark row
    * @param field BigQuery field
    * @return BigQuery field value
    */
  private def getFieldValue(row: Row, field: Field): Any = {
    val idx = row.fieldIndex(field.getName)

    if(row.isNullAt(idx)) {
      null
    } else {
      val sourceType = row.schema(idx).dataType
      val targetType = field.getType

      (sourceType, targetType) match {
        case (BooleanType, BOOLEAN) =>
          row.getBoolean(idx)

        case (ByteType, INTEGER) =>
          row.getByte(idx)

        case (ShortType, INTEGER) =>
          row.getShort(idx)

        case (IntegerType, INTEGER) =>
          row.getInt(idx)

        case (LongType, INTEGER) =>
          row.getLong(idx)

        case (FloatType, FLOAT) =>
          row.getFloat(idx).toDouble

        case (DoubleType, FLOAT) =>
          row.getDouble(idx)

        case (_: DecimalType, NUMERIC) =>
          row.getDecimal(idx)

        case (_: DecimalType, STRING) =>
          row.getDecimal(idx).toPlainString

        case (StringType, STRING) =>
          row.getString(idx)

        case (BinaryType, BYTES) =>
          val bytes = row.getAs[Array[Byte]](idx)
          BaseEncoding.base64().encode(bytes)

        case (StructType(_), RECORD) =>
          val struct = row.getStruct(idx)
          val result = new java.util.HashMap[String, Any](struct.size)

          field.getSubFields.asScala foreach { subField =>
            result.put(subField.getName, getFieldValue(struct, subField))
          }
          result

        case (TimestampType, TIMESTAMP) =>
          // BigQuery requires specifying the number of seconds since the epoch
          DateTime.timestampToEpochSeconds(row.getTimestamp(idx))

        case (DateType, DATE) =>
          DateTime.formatDate(row.getDate(idx))

        case (t: MapType, RECORD) =>
          val m = row.getMap[Any, Any](idx)
          val result = new java.util.HashMap[Any, Any](m.size)

          val (keyField, valueField) = customKeyValueStructFields(t)
          val mapSchema = StructType(Array(keyField, valueField))

          m foreach { kv =>
            val kvRow = new GenericRowWithSchema(kv.productIterator.toArray, mapSchema)
            val keyValue = getFieldValue(kvRow, field.getSubFields.get(0))
            val valueValue = getFieldValue(kvRow, field.getSubFields.get(1))
            result.put(keyValue, valueValue)
          }
          result

        case (st: ArrayType, _) =>
          val arraySchema = StructType(Array(customArrayStructField(st)))
          row.getSeq[Any](idx) map { value =>
            getFieldValue(new GenericRowWithSchema(Array(value), arraySchema), field)
          } asJava
      }
    }
  }
}
