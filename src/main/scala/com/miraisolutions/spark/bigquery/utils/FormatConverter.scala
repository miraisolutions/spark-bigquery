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

package com.miraisolutions.spark.bigquery.utils

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._

/**
  * Provides format conversion utility methods.
  */
object FormatConverter {

  private val PARQUET_LIST_LIST_FIELD_NAME = "list"
  private val PARQUET_LIST_ELEMENT_FIELD_NAME = "element"
  private val PARQUET_MAP_KEYVALUE_FIELD_NAME = "key_value"
  private val PARQUET_MAP_KEY_FIELD_NAME = "key"
  private val PARQUET_MAP_VALUE_FIELD_NAME = "value"

  /**
    * Generic converter partial function for converting data frame columns.
    *
    * The result of a converter is the new field definition/format and a user-defined function (UDF) to transform an
    * existing column into the specified format.
    */
  type Converter = PartialFunction[StructField, (StructField, UserDefinedFunction)]

  /**
    * Transforms a data frame by applying a list of column converters. Converters are applied in the order specified.
    * Columns not matching any of the converter's domains remain unchanged.
    * @param df Input data frame
    * @param converters List of column converters to apply
    * @return Output data frame
    */
  def transform(df: DataFrame, converters: List[Converter]): DataFrame = {
    val convert = converters.reduce(_ orElse _)
    val transformed = df.schema.fields.foldRight((df, List.empty[StructField])) { case (field, (aggDf, aggFields)) =>
      if(convert.isDefinedAt(field)) {
        val (newField, converterUdf) = convert(field)
        val newDf = aggDf.withColumn(field.name, converterUdf(aggDf.col(field.name)))
        (newDf, newField :: aggFields)

      } else {
        // leave column unchanged
        (aggDf, field :: aggFields)
      }
    }
    val (newDf, newFields) = transformed
    df.sqlContext.createDataFrame(newDf.rdd, StructType(newFields))
  }

  /**
    * Creates a Spark UDF to convert a Parquet-List-structured column to an Array column.
    * @param arrayType Resulting array type
    * @return Spark UDF
    * @see [[https://github.com/apache/parquet-format/blob/master/LogicalTypes.md]]
    */
  private def parquetListToArrayUdf(arrayType: ArrayType): UserDefinedFunction = udf((row: Row) => {
    row.getAs[Seq[Row]](PARQUET_LIST_LIST_FIELD_NAME).map(_.getAs[Any](PARQUET_LIST_ELEMENT_FIELD_NAME))
  }, arrayType)

  /**
    * Transform a Parquet-style array-formatted column to a Spark array column.
    * @see [[https://github.com/apache/parquet-format/blob/master/LogicalTypes.md]]
    */
  val parquetListToArray: Converter = {
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
      (newField, parquetListToArrayUdf(arrayType))
  }

  private def parquetMapToMapUdf(mapType: MapType): UserDefinedFunction = udf((row: Row) => {
    val kvPairs = row.getAs[Seq[Row]](PARQUET_MAP_KEYVALUE_FIELD_NAME) map { kv =>
      val key = kv.getAs[Any](PARQUET_MAP_KEY_FIELD_NAME)
      val value = kv.getAs[Any](PARQUET_MAP_VALUE_FIELD_NAME)
      (key, value)
    }
    kvPairs.toMap
  }, mapType)

  val parquetMapToMap: Converter = {
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
      (newField, parquetMapToMapUdf(mapType))
  }
}
