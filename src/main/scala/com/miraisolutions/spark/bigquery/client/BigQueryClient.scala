package com.miraisolutions.spark.bigquery.client

import java.time.{Instant, ZoneId}
import java.time.format.DateTimeFormatter

import com.google.cloud.bigquery.InsertAllRequest.RowToInsert
import com.google.cloud.bigquery.JobInfo.{CreateDisposition, WriteDisposition}
import com.google.cloud.bigquery.{Option => _, _}
import com.miraisolutions.spark.bigquery.utils.SqlLogger
import com.miraisolutions.spark.bigquery.{BigQuerySchemaConverter, BigQueryTableReference}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.language.implicitConversions

private object BigQueryClient {
  private val TEMP_TABLE_PREFIX = "spark_"
  private val TEMP_TABLE_TIMESTAMP_FORMATTER =
    DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(ZoneId.of("UTC"))
}

/**
  * BigQuery Client
  */
// TODO: improve logging
private[bigquery] class BigQueryClient(config: BigQueryConfig) {
  import BigQueryClient._

  // Google BigQuery client using application default credentials
  private val bigquery = BigQueryOptions.getDefaultInstance.getService

  private val logger = LoggerFactory.getLogger(classOf[BigQueryClient])
  private val sqlLogger = SqlLogger(logger)

  private def getOrCreateDataset(project: String, dataset: String)
                                (build: DatasetInfo.Builder => DatasetInfo): Dataset = {
    val datasetId = DatasetId.of(project, dataset)
    Option(bigquery.getDataset(datasetId)).getOrElse {
      val datasetBuilder = DatasetInfo.newBuilder(datasetId)
      val datasetInfo = build(datasetBuilder)
      bigquery.create(datasetInfo)
    }
  }

  private def getOrCreateStagingDataset(): DatasetId = {
    import config._

    // TODO: Cannot read and write in different locations: source: US, destination: EU
    val ds = getOrCreateDataset(project, stagingDataset.name) { builder =>
      builder
        .setDefaultTableLifetime(stagingDataset.lifetime)
        .setDescription(StagingDatasetConfig.DESCRIPTION)
        .setLocation(stagingDataset.location)
        .build()
    }

    ds.getDatasetId
  }

  private def getTemporaryTable(): BigQueryTableReference = {
    import scala.util.Random

    val stagingDataset = getOrCreateStagingDataset()
    val tempTableName = TEMP_TABLE_PREFIX + TEMP_TABLE_TIMESTAMP_FORMATTER.format(Instant.now()) + "_" +
      Random.nextInt(Int.MaxValue)

    BigQueryTableReference(stagingDataset.getProject, stagingDataset.getDataset, tempTableName)
  }

  /**
    * Retrieves the Spark schema for a BigQuery table
    * @param table BigQuery table reference
    * @return Spark schema
    */
  def getSchema(table: BigQueryTableReference): StructType = {
    val schema = bigquery.getTable(table).getDefinition[TableDefinition].getSchema
    logger.info(s"Table $table has schema: $schema")
    BigQuerySchemaConverter.fromBigQueryToSpark(schema)
  }

  def readTable(table: BigQueryTableReference, numPartitions: Int): BigQueryTableResult = {
    val tbl = bigquery.getTable(table)
    new BigQueryTableResult(tbl, tbl.list().getTotalRows, numPartitions)
  }

  def executeQuery(query: String, numPartitions: Int): BigQueryTableResult = {
    sqlLogger.logSqlQuery(query)
    val tempTable = getTemporaryTable()

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
    new BigQueryTableResult(tbl, totalRows, numPartitions)
  }

  private def insertRows(df: DataFrame, table: Table): Unit = {
    val converter = BigQuerySchemaConverter.getSparkToBigQueryConverterFunction(df.schema)
    val rowsToInsert = df.toLocalIterator().asScala.map(converter).map(RowToInsert.of).toIterable.asJava
    val request = InsertAllRequest.newBuilder(table)
      .setRows(rowsToInsert)
      .build()

    val response = bigquery.insertAll(request)
    if(response.hasErrors) {
      val msg = response.getInsertErrors.asScala.values.flatMap(_.asScala.map(_.getMessage)).toSet.mkString("\n")
      throw new RuntimeException(msg)
    }
  }

  def writeTable(df: DataFrame, table: BigQueryTableReference, mode: SaveMode): Unit = {
    import SaveMode._

    // TODO: configurable table location ?
    val ds = getOrCreateDataset(table.project, table.dataset)(_.build())

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
          throw new RuntimeException(s"Table $table already exists and is not empty")
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
}
