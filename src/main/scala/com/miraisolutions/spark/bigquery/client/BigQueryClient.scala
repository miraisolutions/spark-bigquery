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
  // Table prefix for temporary tables
  private val TEMP_TABLE_PREFIX = "spark_"
  // Timestamp formatter for temporary tables
  private val TEMP_TABLE_TIMESTAMP_FORMATTER =
    DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(ZoneId.of("UTC"))
}

/**
  * BigQuery Client
  *
  * @param config BigQuery configuration
  */
private[bigquery] class BigQueryClient(config: BigQueryConfig) {
  import BigQueryClient._

  // Google BigQuery client using application default credentials
  private val bigquery = BigQueryOptions.getDefaultInstance.getService

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
                                (build: DatasetInfo.Builder => DatasetInfo): Dataset = {
    val datasetId = DatasetId.of(project, dataset)
    Option(bigquery.getDataset(datasetId)).getOrElse {
      logger.info(s"Creating dataset '$dataset' in project '$project'")

      val datasetBuilder = DatasetInfo.newBuilder(datasetId)
      val datasetInfo = build(datasetBuilder)
      bigquery.create(datasetInfo)
    }
  }

  /**
    * Retrieve or create staging dataset which hosts temporary SQL query result tables.
    * @return Staging dataset ID
    */
  private def getOrCreateStagingDataset(): DatasetId = {
    import config._

    // TODO: Cannot read and write in different locations: source: US, destination: EU
    // TODO: attach location to dataset name?
    val ds = getOrCreateDataset(project, stagingDataset.name) { builder =>
      builder
        .setDefaultTableLifetime(stagingDataset.lifetime)
        .setDescription(StagingDatasetConfig.DESCRIPTION)
        .setLocation(stagingDataset.location)
        .build()
    }

    ds.getDatasetId
  }

  /**
    * Creates a new (unique) reference to a temporary table which will contain the results of an executed SQL query.
    * @return BigQuery table reference
    */
  private def createTemporaryTableReference(): BigQueryTableReference = {
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
    BigQuerySchemaConverter.fromBigQueryToSpark(schema)
  }

  /**
    * Gets a BigQuery table reader that can be used to read a BigQuery table through a number of pages/partitions.
    * @param table BigQuery table reference
    * @param numPartitions Number of target partitions
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
      val converter = BigQuerySchemaConverter.getSparkToBigQueryConverterFunction(schema)
      val rowsToInsert = rows.map(row => RowToInsert.of(converter(row))).toIterable.asJava
      val response = table.insert(rowsToInsert, false, false)

      if(response.hasErrors) {
        val msg = response.getInsertErrors.asScala.values.flatMap(_.asScala.map(_.getMessage)).toSet.mkString("\n")
        throw new RuntimeException(msg)
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
