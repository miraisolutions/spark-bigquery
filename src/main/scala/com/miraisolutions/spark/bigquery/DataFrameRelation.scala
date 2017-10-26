package com.miraisolutions.spark.bigquery

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.sources.{BaseRelation, PrunedScan, TableScan}
import org.apache.spark.sql.types.StructType

/**
  * Base relation for sources providing Spark DataFrames
  */
private abstract class BaseDataFrameRelation
  extends BaseRelation with TableScan with PrunedScan {
  /** Spark DataFrame */
  protected def dataFrame: DataFrame

  override def schema: StructType = dataFrame.schema

  override def buildScan(): RDD[Row] = dataFrame.rdd

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    if(requiredColumns.length > 0)
      dataFrame.select(requiredColumns.head, requiredColumns.tail: _*).rdd
    else
      dataFrame.rdd
  }
}
