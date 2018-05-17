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
