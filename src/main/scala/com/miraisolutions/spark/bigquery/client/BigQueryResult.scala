package com.miraisolutions.spark.bigquery.client

import com.google.cloud.bigquery.BigQuery.{QueryResultsOption, TableDataListOption}
import com.google.cloud.bigquery.{Job, Table, TableResult}
import com.miraisolutions.spark.bigquery.BigQuerySchemaConverter
import org.apache.spark.sql.Row

import scala.collection.JavaConverters._

abstract class BigQueryResult(val totalRows: Long, val numPartitions: Int) extends Serializable {
  private val pageSize: Long = (totalRows + numPartitions - 1) / numPartitions

  protected def getTableResult(partitionIndex: Int, pageSize: Long): TableResult

  def getRows(partitionIndex: Int): Iterable[Row] = {
    val result = getTableResult(partitionIndex, pageSize)
    val converter = BigQuerySchemaConverter.getBigQueryToSparkConverterFunction(result.getSchema)
    result.iterateAll().asScala.map(converter)
  }
}


class BigQueryTableResult(private[bigquery] val table: Table, totalRows: Long, numPartitions: Int)
  extends BigQueryResult(totalRows, numPartitions) {

  override protected def getTableResult(partitionIndex: Int, pageSize: Long): TableResult = {
    table.list(
      TableDataListOption.pageSize(pageSize),
      TableDataListOption.startIndex(pageSize * partitionIndex)
    )
  }
}

class BigQueryQueryJobResult(private[bigquery] val job: Job, totalRows: Long, numPartitions: Int)
  extends BigQueryResult(totalRows, numPartitions) {

  override protected def getTableResult(partitionIndex: Int, pageSize: Long): TableResult = {
    job.getQueryResults(
      QueryResultsOption.pageSize(pageSize),
      QueryResultsOption.startIndex(pageSize * partitionIndex)
    )
  }
}
