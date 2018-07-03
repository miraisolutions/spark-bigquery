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

package com.miraisolutions.spark.bigquery

import com.miraisolutions.spark.bigquery.client.BigQueryClient
import com.miraisolutions.spark.bigquery.sql.BigQuerySqlGeneration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.slf4j.LoggerFactory

/**
  * Relation for a Google BigQuery table
  *
  * @param sqlContext Spark SQL context
  * @param client BigQuery client
  * @param table BigQuery table reference
  */
private final case class BigQueryTableRelation(sqlContext: SQLContext, client: BigQueryClient,
                                               table: BigQueryTableReference)
  extends BaseRelation with TableScan with PrunedScan with PrunedFilteredScan with InsertableRelation {

  private val logger = LoggerFactory.getLogger(classOf[BigQueryTableRelation])
  private val sql = BigQuerySqlGeneration(table)

  // See {{BaseRelation}}
  override def schema: StructType = client.getSchema(table)

  // See {{TableScan}}
  override def buildScan(): RDD[Row] = {
    logger.info(s"Executing full scan of table $table")
    val tbl = client.getTable(table, sqlContext.sparkContext.defaultParallelism)
    new BigQueryRowRDD(sqlContext.sparkContext, tbl)
  }

  // See {{PrunedScan}}
  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    buildScan(requiredColumns, Array.empty)
  }

  // See {{PrunedFilteredScan}}
  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    logger.info(s"Executing pruned filtered scan of table $table ")
    val sqlQuery = sql.getQuery(requiredColumns, filters)
    val tbl = client.executeQuery(sqlQuery, sqlContext.sparkContext.defaultParallelism)
    new BigQueryRowRDD(sqlContext.sparkContext, tbl)
  }

  // See {{InsertableRelation}}
  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    logger.info(s"Writing to table $table (overwrite = $overwrite)")
    val mode = if(overwrite) SaveMode.Overwrite else SaveMode.Append
    client.writeTable(data, table, mode)
  }
}
