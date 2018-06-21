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

object FormatConverter {

  private val PARQUET_LIST_LIST_FIELD_NAME = "list"
  private val PARQUET_LIST_ELEMENT_FIELD_NAME = "element"

  /**
    * Creates a Spark UDF to convert a Parquet-List-structured column to an Array column.
    * @param arrayType Resulting array type
    * @return Spark UDF
    * @see https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
    */
  private def parquetListToArrayUdf(arrayType: ArrayType): UserDefinedFunction = udf((row: Row) => {
    row.getAs[Seq[Row]](PARQUET_LIST_LIST_FIELD_NAME).map(_.getAs[Any](PARQUET_LIST_ELEMENT_FIELD_NAME))
  }, arrayType)

  /**
    * Transforms any Parquet-style array-formatted columns to Spark array columns. Other columns remain unchanged.
    * @param df Input data frame
    * @return Output data frame
    */
  def parquetListsAsArrays(df: DataFrame): DataFrame = {
    val transformed = df.schema.fields.foldRight((df, List.empty[StructField])) { case (field, (aggDf, aggFields)) =>
      field.dataType match {
        case StructType(
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
              meta
            )
          )
        ) => // this is a field which has the typical Parquet-style array format
          val arrayType = ArrayType(elementType, elementNullable)
          val newField = StructField(field.name, arrayType, field.nullable, meta)
          val newDf = aggDf.withColumn(field.name, parquetListToArrayUdf(arrayType)(aggDf.col(field.name)))
          (newDf, newField :: aggFields)

        case _ => // leave unchanged
          (aggDf, field :: aggFields)
      }
    }

    val (newDf, newFields) = transformed
    df.sqlContext.createDataFrame(newDf.rdd, StructType(newFields))
  }
}
