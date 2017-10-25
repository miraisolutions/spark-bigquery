package com.miraisolutions.spark.bigquery

import com.google.api.services.bigquery.model.TableReference
import com.spotify.spark.bigquery._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCRDD}
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}

/**
 * Relation for Google BigQuery data source
 *
 * @param tableRef BigQuery table reference
 * @param sqlContext Spark SQL context
*/
private[bigquery] case class BigQuerySourceRelation(
    tableRef: TableReference,
    sqlContext: SQLContext,
    jdbcOptions: JDBCOptions)
  extends BaseRelation with PrunedFilteredScan {

  private lazy val table = sqlContext.bigQueryTable(tableRef)

  override def schema: StructType = table.schema

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    JDBCRDD.scanTable(
      sqlContext.sparkSession.sparkContext,
      schema,
      requiredColumns,
      filters,
      table.rdd.partitions,
      jdbcOptions).asInstanceOf[RDD[Row]]
  }
}
