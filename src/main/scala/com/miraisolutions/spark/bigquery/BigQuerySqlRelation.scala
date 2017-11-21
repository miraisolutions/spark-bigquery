/*
 * Copyright (c) 2017 Mirai Solutions GmbH
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

package com.miraisolutions.spark.bigquery

import com.miraisolutions.spark.bigquery.utils.SqlLogger
import com.spotify.spark.bigquery._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}
import org.slf4j.LoggerFactory

/**
  * Relation for a Google BigQuery standard SQL query
  *
  * @param sqlQuery BigQuery standard SQL query in SQL-2011 dialect
  * @param sqlContext Spark SQL context
  */
private final case class BigQuerySqlRelation(sqlQuery: String, sqlContext: SQLContext)
  extends BaseRelation with TableScan {

  private val sqlLogger = SqlLogger(LoggerFactory.getLogger(classOf[BigQuerySqlRelation]))

  private lazy val dataFrame = {
    sqlLogger.logSqlQuery(sqlQuery)
    sqlContext.bigQuerySelect(sqlQuery)
  }

  override def schema: StructType = dataFrame.schema

  override def buildScan(): RDD[Row] = dataFrame.rdd
}
