package com.miraisolutions.spark.bigquery

import com.google.api.services.bigquery.model.TableReference
import com.spotify.spark.bigquery._
import org.apache.spark.sql.sources.InsertableRelation
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
 * Relation for a Google BigQuery table
 *
 * @param tableRef BigQuery table reference
 * @param sqlContext Spark SQL context
 */
private final case class BigQueryTableRelation(
    tableRef: TableReference,
    sqlContext: SQLContext)
  extends BaseDataFrameRelation with InsertableRelation {

  override protected lazy val dataFrame: DataFrame = sqlContext.bigQueryTable(tableRef)

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val writeDisposition = if(overwrite) WriteDisposition.WRITE_TRUNCATE else WriteDisposition.WRITE_APPEND
    data.saveAsBigQueryTable(tableRef, writeDisposition, CreateDisposition.CREATE_NEVER)
  }
}
