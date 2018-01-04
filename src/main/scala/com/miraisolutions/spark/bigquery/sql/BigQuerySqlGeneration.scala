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
