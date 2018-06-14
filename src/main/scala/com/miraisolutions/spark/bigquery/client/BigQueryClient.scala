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

import java.time.{Instant, ZoneId}
import java.time.format.DateTimeFormatter

import com.google.cloud.RetryOption
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert
import com.google.cloud.bigquery.JobInfo.{CreateDisposition, WriteDisposition}
import com.google.cloud.bigquery.{Option => _, _}
import com.miraisolutions.spark.bigquery.config.{BigQueryConfig, StagingDatasetConfig}
import com.miraisolutions.spark.bigquery.exception.IOException
import com.miraisolutions.spark.bigquery.utils.SqlLogger
import com.miraisolutions.spark.bigquery.{BigQuerySchemaConverter, BigQueryTableReference, FileFormat}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.slf4j.LoggerFactory
import org.threeten.bp.Duration

import scala.collection.JavaConverters._
import scala.language.implicitConversions
import scala.util.Random

private object BigQueryClient {
  // Prefix for temporary tables and directories
  private val TEMP_PREFIX = "spark_"

  // Timestamp formatter for temporary tables and GCS staging directories
  private val TIMESTAMP_FORMATTER =
    DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(ZoneId.of("UTC"))
}

/**
  * BigQuery Client
  *
  * @param config BigQuery configuration
  */
private[bigquery] class BigQueryClient(val config: BigQueryConfig) {
  import BigQueryClient._

  // Google BigQuery client using application default credentials
  private val bigquery: BigQuery = BigQueryOptions.getDefaultInstance.getService

  private val logger = LoggerFactory.getLogger(classOf[BigQueryClient])
  private val sqlLogger = SqlLogger(logger)

  /**
    * Retrieves a dataset or creates it if it doesn't exist.
    * @param project Project ID
    * @param dataset Dataset ID
    * @param build Function to configure the dataset to be created if it doesn't exist yet
    * @return BigQuery [[Dataset]]
    */
  private def getOrCreateDataset(project: String, dataset: String)
                                (build: DatasetInfo.Builder => DatasetInfo.Builder): Dataset = {
    val datasetId = DatasetId.of(project, dataset)
    Option(bigquery.getDataset(datasetId)).getOrElse {
      logger.info(s"Creating dataset '$dataset' in project '$project'")

      val datasetBuilder = DatasetInfo.newBuilder(datasetId)
      // New datasets are always created in the configured location
      val datasetInfo = build(datasetBuilder).setLocation(config.location).build()
      bigquery.create(datasetInfo)
    }
  }

  /**
    * Retrieve or create staging dataset which hosts temporary SQL query result tables.
    * @return Staging dataset ID
    */
  private def getOrCreateStagingDataset(): DatasetId = {
    import config._

    val ds = getOrCreateDataset(project, stagingDataset.name + "_" + location) { builder =>
      builder
        .setDefaultTableLifetime(stagingDataset.lifetime)
        .setDescription(StagingDatasetConfig.DESCRIPTION)
    }

    ds.getDatasetId
  }

  /**
    * Creates a temporary name that can be used for temporary tables and directories.
    */
  private def createTempName(): String = {
    TEMP_PREFIX + TIMESTAMP_FORMATTER.format(Instant.now()) + "_" + Random.nextInt(Int.MaxValue)
  }

  /**
    * Creates a new (unique) reference to a temporary table which will contain the results of an executed SQL query.
    * @return BigQuery table reference
    */
  private def createTemporaryTableReference(): BigQueryTableReference = {
    val stagingDataset = getOrCreateStagingDataset()
    val tempTableName = createTempName()

    BigQueryTableReference(stagingDataset.getProject, stagingDataset.getDataset, tempTableName)
  }

  /**
    * Retrieves the Spark schema for a BigQuery table
    * @param table BigQuery table reference
    * @return Spark schema
    */
  def getSchema(table: BigQueryTableReference): StructType = {
    val schema = bigquery.getTable(table).getDefinition[TableDefinition].getSchema
    BigQuerySchemaConverter.fromBigQueryToSpark(schema)
  }

  /**
    * Gets a BigQuery table reader that can be used to read a BigQuery table through a number of pages/partitions.
    * @param table BigQuery table reference
    * @param numPartitions Suggested number of target partitions. The effective number of partitions may be different.
    * @return BigQuery table reader
    */
  def getTable(table: BigQueryTableReference, numPartitions: Int): BigQueryTableReader = {
    val tbl = bigquery.getTable(table)
    BigQueryTableReader(tbl, tbl.list().getTotalRows, numPartitions)
  }

  /**
    * Executes a BigQuery standard SQL query and returns a BigQuery table reader to retrieve the results.
    * @param query BigQuery standard SQL (SQL-2011) query
    * @param numPartitions Number of target partitions
    * @return BigQuery table reader
    */
  def executeQuery(query: String, numPartitions: Int): BigQueryTableReader = {
    sqlLogger.logSqlQuery(query)
    val tempTable = createTemporaryTableReference()

    val queryJobConfiguration =
      QueryJobConfiguration.newBuilder(query)
        .setUseLegacySql(false)
        .setAllowLargeResults(true)
        .setFlattenResults(false)
        .setPriority(config.job.priority)
        .setDestinationTable(tempTable)
        .setCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
        .setWriteDisposition(WriteDisposition.WRITE_EMPTY)
        .build()

    val totalRows = bigquery.query(queryJobConfiguration).getTotalRows
    val tbl = bigquery.getTable(tempTable)
    BigQueryTableReader(tbl, totalRows, numPartitions)
  }

  /**
    * Inserts the rows of a Spark [[DataFrame]] into a BigQuery table.
    * @param df Spark [[DataFrame]]
    * @param table BigQuery table
    */
  private def insertRows(df: DataFrame, table: Table): Unit = {
    // Getting a stable reference to the schema for serialization with the following closure
    val schema = df.schema

    df foreachPartition { rows =>
      if(rows.nonEmpty) {
        val converter = BigQuerySchemaConverter.getSparkToBigQueryConverterFunction(schema)
        val rowsToInsert = rows.map(row => RowToInsert.of(converter(row))).toIterable.asJava
        val response = table.insert(rowsToInsert, false, false)

        if (response.hasErrors) {
          val msg = response.getInsertErrors.asScala.values.flatMap(_.asScala.map(_.getMessage)).toSet.mkString("\n")
          throw new IOException(msg)
        }
      }
    }
  }

  /**
    * Writes a Spark [[DataFrame]] to a BigQuery table.
    * @param df Spark [[DataFrame]]
    * @param table Target BigQuery table
    * @param mode Save mode
    */
  def writeTable(df: DataFrame, table: BigQueryTableReference, mode: SaveMode): Unit = {
    import SaveMode._

    logger.info(s"Attempting to insert ${df.count()} rows to table $table" +
      s" (mode: $mode, partitions: ${df.rdd.getNumPartitions})")

    val ds = getOrCreateDataset(table.project, table.dataset)(identity)

    val schema = BigQuerySchemaConverter.fromSparkToBigQuery(df.schema)

    mode match {
      case Append =>
        val tbl = ds.getOrCreateTable(table.table, schema)
        insertRows(df, tbl)

      case Overwrite =>
        val tbl = ds.dropAndCreateTable(table.table, schema)
        insertRows(df, tbl)

      case ErrorIfExists =>
        if(ds.existsNonEmptyTable(table.table)) {
          throw new IllegalStateException(s"Table $table already exists and is not empty")
        } else {
          val tbl = ds.getOrCreateTable(table.table, schema)
          insertRows(df, tbl)
        }

      case Ignore =>
        if(!ds.existsNonEmptyTable(table.table)) {
          val tbl = ds.getOrCreateTable(table.table, schema)
          insertRows(df, tbl)
        }
    }
  }

  /**
    * Constructs a Google Cloud Storage (GCS) staging directory path that can be used to stage data files for data
    * import and export.
    * @return GCS directory path
    */
  def getStagingDirectory(): String = {
    import config.stagingDataset._

    val tempDirectoryName = createTempName()
    s"gs://$gcsBucket/$name/$tempDirectoryName"
  }

  /**
    * Exports a BigQuery table as a series of files to a temporary directory in a Google Cloud Storage (GCS) bucket.
    * @param table BigQuery table to export
    * @param format File export format
    * @return Temporary GCS staging directory containing the exported files in the specified format
    * @see https://cloud.google.com/bigquery/docs/exporting-data
    */
  def exportTable(table: BigQueryTableReference, format: FileFormat): String = {
    val stagingDirectory = getStagingDirectory()
    val destinationUri = s"$stagingDirectory/${table.table}_*.${format.fileExtension}"

    logger.info(s"Starting export of table $table to $destinationUri (format: $format)")
    val job = bigquery.getTable(table).extract(format.bigQueryFormatOptions.getType, destinationUri)
    waitForJob(job)
    logger.info(s"Done exporting table $table")

    stagingDirectory
  }

  /**
    * Imports data from a Google Cloud Storage (GCS) directory into a BigQuery table.
    * @param path GCS directory path
    * @param format File format
    * @param table BigQuery table reference
    * @param mode Save mode
    */
  def importTable(path: String, format: FileFormat, table: BigQueryTableReference, mode: SaveMode): Unit = {
    import SaveMode._

    getOrCreateDataset(table.project, table.dataset)(identity)

    val baseConfig = LoadJobConfiguration.builder(table, path + s"*.${format.fileExtension}")
      .setAutodetect(true)
      .setIgnoreUnknownValues(false)
      .setMaxBadRecords(0)
      .setFormatOptions(format.bigQueryFormatOptions)
      .setCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)

    val (writeDisposition, ignoreDuplicateError) = mode match {
      case Append =>
        (WriteDisposition.WRITE_APPEND, false)

      case Overwrite =>
        (WriteDisposition.WRITE_TRUNCATE, false)

      case ErrorIfExists =>
        (WriteDisposition.WRITE_EMPTY, false)

      case Ignore =>
        (WriteDisposition.WRITE_EMPTY, true)
    }

    val jobInfo = JobInfo.of(baseConfig.setWriteDisposition(writeDisposition).build())
    val job = bigquery.create(jobInfo)

    logger.info(s"Starting import into table $table from $path (format: $format, mode: $mode)")
    waitForJob(job, ignoreDuplicateError)
    logger.info(s"Done importing into table $table")
  }

  /**
    * Waits for completion of a job and check for errors.
    * @param job Job to wait for
    * @param ignoreDuplicateError Whether to ignore duplicate errors or not
    * @see https://cloud.google.com/bigquery/troubleshooting-errors
    */
  private def waitForJob(job: Job, ignoreDuplicateError: Boolean = false): Unit = {
    val status = job.waitFor(
      RetryOption.initialRetryDelay(Duration.ofSeconds(1)),
      RetryOption.retryDelayMultiplier(1.2),
      RetryOption.maxRetryDelay(Duration.ofSeconds(30)),
      RetryOption.totalTimeout(Duration.ofMillis(config.job.timeout))
    ).getStatus

    if(status.getError != null && (!ignoreDuplicateError || status.getError.getReason != "duplicate")) {
      throw new IOException(s"BigQuery job ${job.getJobId} failed with message: ${status.getError.getMessage}")
    }
  }
}
