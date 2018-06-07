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
