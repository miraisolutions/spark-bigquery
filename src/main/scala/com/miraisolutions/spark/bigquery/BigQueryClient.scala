package com.miraisolutions.spark.bigquery

import com.google.cloud.bigquery.QueryJobConfiguration.Priority
import com.google.cloud.bigquery._
import org.apache.spark.sql.types.StructType

/**
  * BigQuery Client
  */
private object BigQueryClient {

  // Google BigQuery client using application default credentials
  private val bigquery = BigQueryOptions.getDefaultInstance.getService


  /**
    * Retrieves the Spark schema for a BigQuery table
    * @param table BigQuery table reference
    * @return Spark schema
    */
  def getSchema(table: BigQueryTable): StructType = {
    val schema = bigquery.getTable(table).getDefinition[TableDefinition].getSchema
    BigQuerySchemaConverter.fromBigQueryToSpark(schema)
  }

  def getTable(table: BigQueryTable, numPartitions: Int): BigQueryTableResult = {
    val tbl = bigquery.getTable(table)
    new BigQueryTableResult(tbl, tbl.list().getTotalRows, numPartitions)
  }

  def executeQuery(query: String, numPartitions: Int) = {
    val queryJobConfiguration =
      QueryJobConfiguration.newBuilder(query)
        .setUseLegacySql(false)
        .setAllowLargeResults(true)
        .setFlattenResults(false)
        .setPriority(Priority.INTERACTIVE)
        .build()

    val jobId = JobId.newBuilder().setRandomJob().build()
    val result = bigquery.query(queryJobConfiguration, jobId)
    val job = bigquery.getJob(jobId)
    new BigQueryQueryJobResult(job, result.getTotalRows, numPartitions)
  }
}
