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

import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.DataFrame

/**
  * Provides format conversion utility methods.
  */
object FormatConverter {

  /**
    * Transforms a data frame by applying a list of column converters. Converters are applied in the order specified.
    * Columns not matching any of the converter's domains remain unchanged.
    * @param df Input data frame
    * @param converters List of column converters to apply
    * @return Output data frame
    */
  def transform(df: DataFrame, converters: List[ColumnConverter]): DataFrame = {
    val convert = converters.reduce(_ orElse _)
    val transformed = df.schema.fields.foldRight((df, List.empty[StructField])) { case (field, (aggDf, aggFields)) =>
      if(convert.isDefinedAt(field)) {
        val (newField, converterFunction) = convert(field)
        val newDf = aggDf.withColumn(field.name, converterFunction(aggDf.col(field.name)))
        (newDf, newField :: aggFields)

      } else {
        // leave column unchanged
        (aggDf, field :: aggFields)
      }
    }
    val (newDf, newFields) = transformed
    df.sqlContext.createDataFrame(newDf.rdd, StructType(newFields))
  }
}
