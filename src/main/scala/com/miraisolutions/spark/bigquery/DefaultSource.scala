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

import com.google.cloud.hadoop.fs.gcs.{GoogleHadoopFS, GoogleHadoopFileSystem}
import com.miraisolutions.spark.bigquery.FileFormat.CSV
import com.miraisolutions.spark.bigquery.client.BigQueryClient
import com.miraisolutions.spark.bigquery.config.BigQueryConfig
import com.miraisolutions.spark.bigquery.exception.MissingParameterException
import com.miraisolutions.spark.bigquery.utils.Files
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.execution.FileRelation

/**
  * Google BigQuery default data source.
  */
class DefaultSource extends RelationProvider with CreatableRelationProvider with DataSourceRegister {
  import DefaultSource._

  // See {{DataSourceRegister}}
  override def shortName(): String = BIGQUERY_DATA_SOURCE_NAME

  // See {{RelationProvider}}
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    withBigQueryClient(sqlContext, parameters, false) { (client, table) =>
      parameters.foldType[BaseRelation](BigQueryTableRelation(sqlContext, client, table)) { format =>
        val stagingDirectory = client.exportTable(table, format)
        // Register staging directory for deletion when FileSystem gets closed
        Files.deleteOnExit(stagingDirectory, sqlContext.sparkContext.hadoopConfiguration)

        getStagingDataFileRelation(sqlContext, stagingDirectory, format)
      }
    }
  }

  // See {{CreatableRelationProvider}}
  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String],
                              data: DataFrame): BaseRelation = {

    withBigQueryClient(sqlContext, parameters, true) { (client, table) =>
      parameters.foldType[Unit](client.writeTable(data, table, mode)) { format =>
        val stagingDirectory = client.getStagingDirectory()

        // Use TIMESTAMP_MICROS in Parquet (supported since Spark 2.3.0)
        sqlContext.setConf("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")

        data.write
          .format(format.sparkFormatIdentifier)
          .options(getFormatOptions(format))
          .save(stagingDirectory)

        client.importTable(stagingDirectory, format, table, mode)

        // Remove staging directory after import has been completed
        Files.delete(stagingDirectory, sqlContext.sparkContext.hadoopConfiguration)
      }

      BigQueryTableRelation(sqlContext, client, table)
    }
  }
}

private[bigquery] object DefaultSource {

  // BigQuery data source name
  val BIGQUERY_DATA_SOURCE_NAME = "bigquery"

  // Direct import/export type
  private val TYPE_DIRECT = "direct"

  /** Creates a BigQuery client with the provided configuration parameters */
  private def getBigQueryClient(parameters: Map[String, String]): BigQueryClient = {
    new BigQueryClient(BigQueryConfig(parameters))
  }

  /**
    * Sets several necessary Spark Hadoop configuration options to enable access to Google Cloud Storage (GCS).
    * @param conf Spark Hadoop configuration
    * @param project Google Cloud project
    * @param serviceAccountKeyFile Optional Google Cloud service account key file
    * @see [[https://cloud.google.com/storage/docs/authentication#service_accounts]]
    * @see [[https://github.com/GoogleCloudPlatform/bigdata-interop/blob/master/gcs/INSTALL.md]]
    */
  private def initHadoop(conf: Configuration, project: String, serviceAccountKeyFile: Option[String]): Unit = {
    conf.set("fs.gs.impl", classOf[GoogleHadoopFileSystem].getName)
    conf.set("fs.AbstractFileSystem.gs.impl", classOf[GoogleHadoopFS].getName)
    conf.set("fs.gs.project.id", project)

    serviceAccountKeyFile foreach { file =>
      conf.set("google.cloud.auth.service.account.enable", "true")
      conf.set("google.cloud.auth.service.account.json.keyfile", file)
    }
  }

  /**
    * Determines Spark options to be provided for a particular file format
    * @param format File format
    * @return Spark import/export options
    */
  private def getFormatOptions(format: FileFormat): Map[String, String] = {
    format match {
      case CSV =>
        Map("header" -> "true")

      case _ =>
        Map.empty
    }
  }

  /**
    * Creates a Spark [[FileRelation]] for staged data files in a Google Cloud Storage (GCS) staging directory.
    * @param sqlContext Spark SQL context
    * @param stagingDirectory Staging directory path
    * @param format File export format
    * @return Spark [[BaseRelation]] for the staged data files
    */
  private def getStagingDataFileRelation(sqlContext: SQLContext, stagingDirectory: String,
                                         format: FileFormat): BaseRelation = {
    val dataSource = DataSource(
      sparkSession = sqlContext.sparkSession,
      className = format.sparkFormatIdentifier,
      paths = List(stagingDirectory),
      userSpecifiedSchema = None,
      options = getFormatOptions(format)
    )

    dataSource.resolveRelation(true)
  }

  /**
    * Gets a BigQuery table reference for the specified parameters. The parameters must either specify a table or
    * a SQL query.
    * @param sqlContext Spark SQL context
    * @param client BigQuery client
    * @param parameters Parameters - must either specify a table or SQL query.
    * @param tableOnly Specifies whether a direct table reference is required.
    * @return Reference to a BigQuery table that holds the data
    */
  private def getBigQueryTableReference(sqlContext: SQLContext, client: BigQueryClient,
                                        parameters: Map[String, String], tableOnly: Boolean): BigQueryTableReference = {
    // Get direct table reference if 'table' has been specified
    val tableOpt = parameters.get("table").map(BigQueryTableReference(_))

    if(tableOnly) {
      tableOpt.getOrElse(throw new MissingParameterException(
        "A parameter 'table' of the form [projectId].[datasetId].[tableId] must be specified."
      ))
    } else {
      // Execute 'sqlQuery' and get reference to table containing the results
      def sqlTableOpt: Option[BigQueryTableReference] = parameters.get("sqlQuery") map { sqlQuery =>
        client.executeQuery(sqlQuery, sqlContext.sparkContext.defaultParallelism).table
      }

      tableOpt.orElse(sqlTableOpt).getOrElse(throw new MissingParameterException(
        "Either a parameter 'table' of the form [projectId].[datasetId].[tableId] or 'sqlQuery' must be specified."
      ))
    }
  }

  /**
    * Constructs a BigQuery client, applies the necessary Spark Hadoop configuration and then calls a provided
    * function to create a [[BaseRelation]].
    * @param sqlContext Spark SQL context
    * @param parameters Parameters
    * @param tableOnly Specifies whether a direct table reference is required.
    * @param createRelation Function to create a [[BaseRelation]] given a BigQuery client and a table reference.
    * @return Spark [[BaseRelation]]
    */
  private def withBigQueryClient(sqlContext: SQLContext, parameters: Map[String, String], tableOnly: Boolean)
                                (createRelation:
                                 (BigQueryClient, BigQueryTableReference) => BaseRelation): BaseRelation = {

    val client = getBigQueryClient(parameters)

    initHadoop(sqlContext.sparkContext.hadoopConfiguration, client.config.project,
      client.config.serviceAccountKeyFile)

    val tableReference = getBigQueryTableReference(sqlContext, client, parameters, tableOnly)

    createRelation(client, tableReference)
  }

  /** Helper class for parameter handling */
  private implicit class TypeParameter(parameters: Map[String, String]) {
    /**
      * Fold on import/export type.
      * @param direct Block to execute when 'type' = 'direct'
      * @param handleFileFormat Function to execute when 'type' = <supported format>
      */
    def foldType[T](direct: => T)(handleFileFormat: FileFormat => T): T = {
      parameters.getOrElse("type", TYPE_DIRECT) match {
        case TYPE_DIRECT =>
          direct

        case tpe =>
          handleFileFormat(FileFormat(tpe))
      }
    }
  }
}
