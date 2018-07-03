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

package com.miraisolutions.spark.bigquery.client

import com.google.cloud.bigquery.BigQuery.TableDataListOption
import com.google.cloud.bigquery._
import com.miraisolutions.spark.bigquery.{BigQuerySchemaConverter, BigQueryTableReference}
import org.apache.spark.sql.Row
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

private[bigquery] object BigQueryTableReader {
  // Maximum number of rows per BigQuery page. See https://cloud.google.com/bigquery/docs/paging-results
  private val MAX_ROWS_PER_PAGE: Long = 100000L
}

/**
  * Table reader used to read a BigQuery table through a number of pages/partitions.
  * @param table BigQuery table
  * @param totalRows Total number of rows to read (across all pages)
  * @param suggestedNumPartitions Suggested number of target partitions. The effective number of partitions may
  *                               be different.
  * @see [[https://cloud.google.com/bigquery/docs/paging-results]]
  */
private[bigquery] case class BigQueryTableReader private (table: Table, totalRows: Long, suggestedNumPartitions: Int) {
  import BigQueryTableReader._

  private val logger = LoggerFactory.getLogger(classOf[BigQueryTableReader])

  // BigQuery => Spark schema converter
  private lazy val converter: FieldValueList => Row = {
    val schema = table.getDefinition[TableDefinition].getSchema
    BigQuerySchemaConverter.getBigQueryToSparkConverterFunction(schema)
  }

  // Page size to use when reading from BigQuery; note that there is a limit of 100k rows per page
  private val pageSize: Long = {
    val suggestedPageSize = (totalRows + suggestedNumPartitions - 1) / suggestedNumPartitions
    Math.min(Math.min(suggestedPageSize, MAX_ROWS_PER_PAGE), totalRows)
  }

  /**
    * The effective number of partitions. This may be different from the `suggestedNumPartitions`.
    */
  def numPartitions: Int = Math.ceil(totalRows.toDouble / pageSize).toInt

  /**
    * Reads a page/partition of a specified size.
    * @param pageIndex Page index
    * @return BigQuery [[TableResult]] that can be used to iterate through the results
    */
  private def getTableResult(pageIndex: Int): TableResult = {
    table.list(
      TableDataListOption.pageSize(pageSize),
      TableDataListOption.startIndex(pageSize * pageIndex)
    )
  }

  /**
    * Get a row iterable for the specified partition.
    * @param partitionIndex Partition index
    * @return Row iterable
    */
  def getRows(partitionIndex: Int): Iterable[Row] = {
    logger.info(s"Retrieving rows of table ${BigQueryTableReference(table.getTableId)} partition $partitionIndex" +
      s" (page size: $pageSize, total rows: $totalRows, partitions: $numPartitions)")

    val result = getTableResult(partitionIndex)
    result.getValues.asScala.map(converter)
  }
}
