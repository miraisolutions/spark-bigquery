package com.miraisolutions.spark.bigquery

import com.google.cloud.bigquery._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import java.sql.{Date, Timestamp}
import java.time.Instant

import scala.collection.JavaConverters._

private object BigQuerySchemaConverter {

  def fromBigQueryToSpark(schema: Schema): StructType = {
    val fields = schema.getFields.asScala.map(bigQueryToSparkField)
    StructType(fields)
  }

  def getConverterFunction(schema: Schema): FieldValueList => Row = { fields =>
    val meta = fromBigQueryToSpark(schema).fields.zip(fields.asScala)
    val values = meta map { case (field, value) => getRowValue(value, field.dataType) }
    Row.fromSeq(values)
  }

  private def bigQueryToSparkField(field: Field): StructField = {
    import LegacySQLTypeName._

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
          Date.valueOf(value.getStringValue)
      }
    }
  }
}
