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

/**
  * Table reader used to read a BigQuery table through a number of pages/partitions.
  * @param table BigQuery table
  * @param totalRows Total number of rows to read (across all pages)
  * @param numPartitions Number of target partitions
  */
private[bigquery] case class BigQueryTableReader private (table: Table, totalRows: Long, numPartitions: Int) {

  private val logger = LoggerFactory.getLogger(classOf[BigQueryTableReader])

  // Page size to use when reading from BigQuery
  private val pageSize: Long = (totalRows + numPartitions - 1) / numPartitions

  // BigQuery => Spark schema converter
  private lazy val converter: FieldValueList => Row = {
    val schema = table.getDefinition[TableDefinition].getSchema
    BigQuerySchemaConverter.getBigQueryToSparkConverterFunction(schema)
  }

  /**
    * Reads a page/partition of a specified size.
    * @param partitionIndex Partition index
    * @param pageSize Page size
    * @return BigQuery [[TableResult]] that can be used to iterate through the results
    */
  private def getTableResult(partitionIndex: Int, pageSize: Long): TableResult = {
    table.list(
      TableDataListOption.pageSize(pageSize),
      TableDataListOption.startIndex(pageSize * partitionIndex)
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

    val result = getTableResult(partitionIndex, pageSize)
    result.getValues.asScala.map(converter)
  }
}
