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

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{ArrayType, MapType, StructField, StructType}

/**
  * Apache Parquet format converters.
  *
  * @see [[https://github.com/apache/parquet-format/blob/master/LogicalTypes.md]]
  */
object Parquet {

  private val PARQUET_LIST_LIST_FIELD_NAME = "list"
  private val PARQUET_LIST_ELEMENT_FIELD_NAME = "element"
  private val PARQUET_MAP_KEYVALUE_FIELD_NAME = "key_value"
  private val PARQUET_MAP_KEY_FIELD_NAME = "key"
  private val PARQUET_MAP_VALUE_FIELD_NAME = "value"

  /**
    * Creates a Spark UDF to convert a Parquet-LIST-structured column to a Spark array column.
    * @param arrayType Resulting array type
    * @return Spark UDF
    */
  private def parquetListToArrayUdf(arrayType: ArrayType): UserDefinedFunction = udf((row: Row) => {
    row.getAs[Seq[Row]](PARQUET_LIST_LIST_FIELD_NAME).map(_.getAs[Any](PARQUET_LIST_ELEMENT_FIELD_NAME))
  }, arrayType)

  /**
    * Transforms a Parquet-LIST-structured column to a Spark array column.
    */
  val parquetListToArray: ColumnConverter = {
    case StructField(
      name,
      StructType(
        Array(
          StructField( // Parquet: repeated group list
            PARQUET_LIST_LIST_FIELD_NAME,
            ArrayType(
              StructType(
                Array(
                  // Parquet: element field
                  StructField(PARQUET_LIST_ELEMENT_FIELD_NAME, elementType, elementNullable, _)
                )
              ),
              false
            ),
            false, // repeated fields are not nullable
            _
          )
        )
      ),
      nullable,
      meta
    ) =>
      val arrayType = ArrayType(elementType, elementNullable)
      val newField = StructField(name, arrayType, nullable, meta)
      (newField, parquetListToArrayUdf(arrayType)(_))
  }

  /**
    * Creates a Spark UDF to convert a Parquet-MAP-structured column to a Spark map column.
    * @param mapType Resulting map type
    * @return Spark UDF
    */
  private def parquetMapToMapUdf(mapType: MapType): UserDefinedFunction = udf((row: Row) => {
    val kvPairs = row.getAs[Seq[Row]](PARQUET_MAP_KEYVALUE_FIELD_NAME) map { kv =>
      val key = kv.getAs[Any](PARQUET_MAP_KEY_FIELD_NAME)
      val value = kv.getAs[Any](PARQUET_MAP_VALUE_FIELD_NAME)
      (key, value)
    }
    kvPairs.toMap
  }, mapType)

  /**
    * Transforms a Parquet-MAP-structured column to a Spark map column.
    */
  val parquetMapToMap: ColumnConverter = {
    case StructField(
      name,
      StructType(
        Array(
          StructField(
            PARQUET_MAP_KEYVALUE_FIELD_NAME,
            ArrayType(
              StructType(
                Array(
                  StructField(PARQUET_MAP_KEY_FIELD_NAME, keyType, false, _),
                  StructField(PARQUET_MAP_VALUE_FIELD_NAME, valueType, valueNullable, _)
                )
              ),
              false
            ),
            false,
            _
          )
        )
      ),
      nullable,
      meta
    ) =>
      val mapType = MapType(keyType, valueType, valueNullable)
      val newField = StructField(name, mapType, nullable, meta)
      (newField, parquetMapToMapUdf(mapType)(_))
  }
}
