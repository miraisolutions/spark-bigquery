package com.miraisolutions.spark.bigquery

import com.google.cloud.bigquery.BigQuery.{QueryResultsOption, TableDataListOption}
import com.google.cloud.bigquery.{Job, Table, TableResult}
import org.apache.spark.sql.Row

import scala.collection.JavaConverters._

private abstract class BigQueryResult(totalRows: Long, numPartitions: Int) {
  private val pageSize: Long = (totalRows + numPartitions - 1) / numPartitions

  protected def getTableResult(partitionIndex: Int, pageSize: Long): TableResult

  def getRows(partitionIndex: Int): Iterable[Row] = {
    val result = getTableResult(partitionIndex, pageSize)
    val converter = BigQuerySchemaConverter.getConverterFunction(result.getSchema)
    result.iterateAll().asScala.map(converter)
  }
}


private class BigQueryTableResult(table: Table, totalRows: Long, numPartitions: Int)
  extends BigQueryResult(totalRows, numPartitions) {

  override protected def getTableResult(partitionIndex: Int, pageSize: Long): TableResult = {
    table.list(
      TableDataListOption.pageSize(pageSize),
      TableDataListOption.startIndex(pageSize * partitionIndex)
    )
  }
}

private class BigQueryQueryJobResult(job: Job, totalRows: Long, numPartitions: Int)
  extends BigQueryResult(totalRows, numPartitions) {

  override protected def getTableResult(partitionIndex: Int, pageSize: Long): TableResult = {
    job.getQueryResults(
      QueryResultsOption.pageSize(pageSize),
      QueryResultsOption.startIndex(pageSize * partitionIndex)
    )
  }
}
