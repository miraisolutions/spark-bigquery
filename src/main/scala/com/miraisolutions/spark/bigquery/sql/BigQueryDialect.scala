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
