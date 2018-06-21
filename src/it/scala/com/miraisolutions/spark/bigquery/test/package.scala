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

import java.sql.Timestamp

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{base64, explode_outer, udf}
import org.apache.spark.sql.types._

package object test {
  import BigQuerySchemaConverter.BIGQUERY_NUMERIC_DECIMAL

  // Rounds a timestamp to milliseconds
  private def roundTimestampToMillis(ts: Timestamp): Timestamp = {
    val roundedTs = new Timestamp(ts.getTime)
    roundedTs.setNanos(Math.round(ts.getNanos / 1e6).toInt)
    roundedTs
  }

  // Spark SQL UDF to round timestamps to milliseconds
  private val roundTimestampToMillisUdf = udf(roundTimestampToMillis _)


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
      * @see [[https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types]]
      */
    def aligned: DataFrame = {
      dataFrame.schema.fields.foldLeft(dataFrame) { case (df, field) =>
        field.dataType match {
          case ByteType | ShortType | IntegerType =>
            cast(df, field.name, LongType)

          case FloatType =>
            cast(df, field.name, DoubleType)

          case dt: DecimalType if dt.precision < 38 =>
            // NOTE: Casting to decimal changes the nullable property of the column;
            // we therefore need to reset the original nullable property;
            // see https://stackoverflow.com/q/50854815
            setNullable(cast(df, field.name, BIGQUERY_NUMERIC_DECIMAL), field.name, field.nullable)

          case TimestampType =>
            // See https://github.com/GoogleCloudPlatform/google-cloud-java/issues/3356
            transform(df, field.name, roundTimestampToMillisUdf)

          case BinaryType =>
            // Array[Byte] cannot be compared easily so we encode in Base64
            df.withColumn(field.name, base64(df.col(field.name)))

          case _: ArrayType =>
            explode(df, field.name).aligned

          case _: MapType =>
            explode(df, field.name, _.as(Seq(field.name + "_key", field.name + "_value"))).aligned

          case _ =>
            df
        }
      }
    }
  }

  // Casts a column in a data frame to a target type
  private def cast(df: DataFrame, columnName: String, dataType: DataType): DataFrame = {
    df.withColumn(columnName, df.col(columnName).cast(dataType))
  }

  // Applies a column transformation using a UDF
  private def transform[A, B](df: DataFrame, columnName: String, f: UserDefinedFunction): DataFrame = {
    df.withColumn(columnName, f(df.col(columnName)))
  }

  // Sets the nullable property of a column
  private def setNullable(df: DataFrame, columnName: String, nullable: Boolean): DataFrame = {
    val newFields = df.schema.fields map {
      case field: StructField if field.name == columnName =>
        field.copy(nullable = nullable)

      case field =>
        field
    }
    df.sqlContext.createDataFrame(df.rdd, StructType(newFields))
  }

  // Explodes a nested column such as an array or map column
  private def explode(df: DataFrame, columnName: String, f: Column => Column = identity): DataFrame = {
    df.select(df.col("*"), f(explode_outer(df.col(columnName)))).drop(columnName)
  }
}
