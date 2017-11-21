package com.miraisolutions.spark.bigquery.sql

import org.apache.spark.sql.jdbc.JdbcDialect

/**
  * Google BigQuery SQL-2011 dialect
  */
private case object BigQueryDialect extends JdbcDialect {

  // BigQuery standard SQL query prefix;
  // see https://cloud.google.com/bigquery/docs/reference/standard-sql/enabling-standard-sql
  val QUERY_PREFIX = "#standardSQL\n"

  override def canHandle(url: String): Boolean = false

  override def quoteIdentifier(colName: String): String = s"`$colName`"

  override def getTableExistsQuery(table: String): String = s"${QUERY_PREFIX} SELECT 1 FROM $table LIMIT 1"

  override def getSchemaQuery(table: String): String = s"${QUERY_PREFIX} SELECT * FROM $table LIMIT 1"
}
