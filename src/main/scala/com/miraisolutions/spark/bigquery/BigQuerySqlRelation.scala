package com.miraisolutions.spark.bigquery

import com.spotify.spark.bigquery._
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Relation for a Google BigQuery standard SQL query
  *
  * @param sqlQuery BigQuery standard SQL query in SQL-2011 dialect
  * @param sqlContext Spark SQL context
  */
private final case class BigQuerySqlRelation(
    sqlQuery: String,
    sqlContext: SQLContext)
  extends BaseDataFrameRelation {

  override protected lazy val dataFrame: DataFrame = sqlContext.bigQuerySelect(sqlQuery)
}
