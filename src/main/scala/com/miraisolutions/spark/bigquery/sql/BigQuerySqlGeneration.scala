package com.miraisolutions.spark.bigquery.sql

import com.google.api.services.bigquery.model.TableReference
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRDD
import org.apache.spark.sql.sources.Filter


/**
  * Google BigQuery SQL Generation
  * @param tableRef BigQuery table reference
  * @see https://cloud.google.com/bigquery/docs/reference/standard-sql/
  */
private[bigquery] case class BigQuerySqlGeneration(tableRef: TableReference) {

  // BigQuery quoted table name
  lazy val table: String = {
    import tableRef._
    BigQueryDialect.quoteIdentifier(s"$getProjectId.$getDatasetId.$getTableId")
  }

  /** SQL query used to determine schema */
  def getSchemaQuery: String = BigQueryDialect.getSchemaQuery(table)

  // Generates column list for SELECT statement
  private def getColumnList(columns: Array[String]): String = {
    if(columns.isEmpty) {
      "1"
    } else {
      columns.map(BigQueryDialect.quoteIdentifier).mkString(",")
    }
  }

  // Generates the filter expressions in a WHERE clause
  private def getWhereClauseFilters(filters: Array[Filter]): String = {
    filters
      .flatMap(JDBCRDD.compileFilter(_, BigQueryDialect))
      .map(p => s"($p)")
      .mkString(" AND ")
  }

  def getQuery(columns: Array[String]): String = {
    import BigQueryDialect._
    s"${QUERY_PREFIX} SELECT ${getColumnList(columns)} FROM $table"
  }

  def getQuery(columns: Array[String], filters: Array[Filter]): String = {
    val whereClause = if(filters.isEmpty) "" else " WHERE " + getWhereClauseFilters(filters)
    getQuery(columns) + whereClause
  }
}
