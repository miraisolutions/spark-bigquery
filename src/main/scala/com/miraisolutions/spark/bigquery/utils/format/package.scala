package com.miraisolutions.spark.bigquery.utils

import org.apache.spark.sql.Column
import org.apache.spark.sql.types.StructField

package object format {

  /**
    * Generic converter partial function for converting data frame columns.
    *
    * The result of a converter is the new field definition/format and a column-to-column function to transform an
    * existing column into the specified format.
    */
  type ColumnConverter = PartialFunction[StructField, (StructField, Column => Column)]

}
