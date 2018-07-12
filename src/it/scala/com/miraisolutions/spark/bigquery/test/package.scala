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

import com.miraisolutions.spark.bigquery.utils.format._
import java.sql.Timestamp

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{base64, explode_outer, udf}
import org.apache.spark.sql.types._

package object test {
  import BigQuerySchemaConverter.BIGQUERY_NUMERIC_DECIMAL

  // Column flattener: partial function from field to flattened fields and an appropriate converter function
  private type ColumnFlattener = PartialFunction[StructField, (Seq[StructField], DataFrame => DataFrame)]

  // Rounds a timestamp to milliseconds
  private def roundTimestampToMillis(ts: Timestamp): Timestamp = {
    val roundedTs = new Timestamp(ts.getTime)
    roundedTs.setNanos(Math.round(ts.getNanos / 1e6).toInt)
    roundedTs
  }

  // Spark SQL UDF to round timestamps to milliseconds
  private val roundTimestampToMillisUdf = udf(roundTimestampToMillis _)


  // Explodes a nested column such as an array or map column
  private def explode(df: DataFrame, columnName: String, f: Column => Column = identity): DataFrame = {
    df.select(df.col("*"), f(explode_outer(df.col(columnName)))).drop(columnName)
  }

  // Column converter used to convert atomic data types into data types supported in BigQuery
  private val atomicTypeConverter: ColumnConverter = {
    case f @ StructField(_, ByteType | ShortType | IntegerType, _, _) =>
      (f.copy(dataType = LongType), _.cast(LongType))

    case f @ StructField(_, FloatType, _, _) =>
      (f.copy(dataType = DoubleType), _.cast(DoubleType))

    case f @ StructField(_, dt: DecimalType, _, _) if dt.precision < 38 =>
      (f.copy(dataType = BIGQUERY_NUMERIC_DECIMAL), _.cast(BIGQUERY_NUMERIC_DECIMAL))

    case f @ StructField(_, TimestampType, _, _) =>
      (f, roundTimestampToMillisUdf(_))

    case f @ StructField(_, BinaryType, _, _) =>
      (f.copy(dataType = StringType), base64)
  }

  // Column flattener for nested data types
  private val nestedTypeFlattener: ColumnFlattener = {
    case StructField(name, ArrayType(elementType, containsNull), _, _) =>
      val newName = name + "_element"
      (Seq(StructField(newName, elementType, containsNull)), explode(_, name, _.as(newName)))

    case StructField(name, MapType(keyType, valueType, containsNull), _, _) =>
      val keyName = name + "_key"
      val valueName = name + "_value"
      (Seq(StructField(keyName, keyType, false), StructField(valueName, valueType, containsNull)),
        explode(_, name, _.as(Seq(keyName, valueName))))

    case StructField(name, StructType(fields), _, _) =>
      def unnest(df: DataFrame): DataFrame = {
        val subFields = df.col("*") :: (fields.toList map { sub =>
          df.col(name).getItem(sub.name).as(name + "_" + sub.name)
        })
        df.select(subFields: _*).drop(name)
      }
      (fields.toSeq, unnest)
  }

  /**
    * Implicit helper class to align column types in a data frame to types supported in BigQuery and types
    * suitable for comparison.
    * @param dataFrame Source data frame
    */
  private[bigquery] implicit class AlignedDataFrame(val dataFrame: DataFrame) extends AnyVal {

    /**
      * Converts/casts columns to the appropriate types supported in BigQuery and types which are suitable for
      * comparison. Specifically:
      *
      * - BigQuery only supports 8 byte integer and floating point types.
      *
      * - BigQuery only supports decimal types with precision 38 and scale 9
      *
      * - Binary types are converted to base64 encoded strings for comparison
      *
      * - Array and map types are exploded for easier comparison
      *
      * - Struct types are unfolded
      *
      * - Parquet-style formatted columns (LIST, MAP) are converted to their native Spark equivalent
      *
      * @see [[https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types]]
      * @see [[FormatConverter]], [[Parquet]]
      */
    def aligned: DataFrame = {
      import Parquet._
      import Generic._

      var df: DataFrame = dataFrame

      do {
        df = FormatConverter.transform(df, List(parquetListToArray, parquetMapToMap, keyValueRecordToMap))
        df = df.schema.fields.foldLeft(df) { case (agg, field) =>
          if(nestedTypeFlattener.isDefinedAt(field)) {
            val (_, converter) = nestedTypeFlattener(field)
            converter(agg)
          } else {
            agg
          }
        }
      } while(df.schema.fields.exists(nestedTypeFlattener.isDefinedAt))

      FormatConverter.transform(df, List(atomicTypeConverter))
    }
  }
}
