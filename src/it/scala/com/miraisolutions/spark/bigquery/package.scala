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

package com.miraisolutions.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

package object bigquery {

  /**
    * Implicit helper class to align column types in a data frame to types supported in BigQuery
    * @param dataFrame Source data frame
    */
  implicit class AlignedDataFrame(val dataFrame: DataFrame) extends AnyVal {
    /**
      * Converts/casts columns to the appropriate types supported in BigQuery
      * @return
      */
    def aligned: DataFrame = {
      dataFrame.schema.fields.foldLeft(dataFrame) { case (df, field) =>
        field.dataType match {
          case ByteType | ShortType | IntegerType =>
            cast(df, field.name, LongType)

          case FloatType =>
            cast(df, field.name, DoubleType)

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
}
