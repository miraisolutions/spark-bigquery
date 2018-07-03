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

package com.miraisolutions.spark.bigquery.utils.format

import com.miraisolutions.spark.bigquery.BigQuerySchemaConverter
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{ArrayType, MapType, StructField, StructType}
import scala.collection.mutable.WrappedArray

/**
  * Generic format converters.
  */
object Generic {
  import BigQuerySchemaConverter._

  /**
    * Creates a Spark UDF to convert an array of key-value structs to a Spark map column.
    * @param mapType Resulting map type
    * @return Spark UDF
    */
  private def keyValueRecordToMapUdf(mapType: MapType): UserDefinedFunction = udf((kvMap: WrappedArray[Row]) => {
    val kvPairs = kvMap map { kvRecord =>
      val key = kvRecord.getAs[Any](KEY_FIELD_NAME)
      val value = kvRecord.getAs[Any](VALUE_FIELD_NAME)
      (key, value)
    }
    kvPairs.toMap
  }, mapType)

  /**
    * Transforms a BigQuery repeated record of key-value fields to a Spark map column.
    */
  val keyValueRecordToMap: ColumnConverter = {
    case StructField(
      name,
      ArrayType(
        StructType(
          Array(
            StructField(KEY_FIELD_NAME, keyType, false, _), // key field
            StructField(VALUE_FIELD_NAME, valueType, valueNullable, _) // value field
          )
        ),
        false
      ),
      nullable,
      meta
    ) =>
      val mapType = MapType(keyType, valueType, valueNullable)
      val newField = StructField(name, mapType, nullable, meta)
      (newField, keyValueRecordToMapUdf(mapType)(_))
  }
}
