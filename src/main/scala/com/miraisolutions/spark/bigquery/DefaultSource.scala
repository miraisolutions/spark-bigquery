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

import com.miraisolutions.spark.bigquery.client.{BigQueryClient, BigQueryConfig}
import com.miraisolutions.spark.bigquery.exception.MissingParameterException
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.execution.FileRelation

/**
  * Google BigQuery default data source
  */
class DefaultSource extends RelationProvider with CreatableRelationProvider with DataSourceRegister {

  /** Short name for data source */
  override def shortName(): String = "bigquery"

  /** Creates a BigQuery client with the provided configuration parameters */
  private def getBigQueryClient(parameters: Map[String, String]): BigQueryClient = {
    new BigQueryClient(BigQueryConfig(parameters))
  }

  /**
    * Exports a BigQuery table to a GCS staging location and creates a corresponding Spark [[FileRelation]].
    * @param sqlContext Spark SQL context
    * @param client BigQuery client
    * @param table BigQuery table reference
    * @param format File export format
    * @return Spark [[BaseRelation]] for the exported data files
    */
  private def getTableExportFileRelation(sqlContext: SQLContext, client: BigQueryClient,
                                         table: BigQueryTableReference, format: FileExportFormat): BaseRelation = {
    val stagingDirectory = client.exportTable(table, format)
    val dataSource = DataSource(
      sparkSession = sqlContext.sparkSession,
      className = format.sparkFormatIdentifier,
      paths = List(stagingDirectory),
      userSpecifiedSchema = None
    )
    dataSource.resolveRelation(true)
  }

  /**
    * Gets a BigQuery table reference for the specified parameters. The parameters must either specify a table or
    * a SQL query.
    * @param sqlContext Spark SQL context
    * @param client BigQuery client
    * @param parameters Parameters - must either specify a table or SQL query.
    * @return Reference to a BigQuery table that holds the data
    */
  private def getBigQueryTableReference(sqlContext: SQLContext, client: BigQueryClient,
                                        parameters: Map[String, String]): BigQueryTableReference = {
    // Get direct table reference if 'table' has been specified
    val table: Option[BigQueryTableReference] = parameters.get("table").map(BigQueryTableReference(_))
    // Execute 'sqlQuery' and get reference to table containing the results
    def sqlTable: Option[BigQueryTableReference] = parameters.get("sqlQuery") map { sqlQuery =>
      client.executeQuery(sqlQuery, sqlContext.sparkContext.defaultParallelism).table
    }

    table.orElse(sqlTable).getOrElse(throw new MissingParameterException(
      "Either a parameter 'table' of the form [projectId].[datasetId].[tableId] or 'sqlQuery' must be specified."
    ))
  }

  // See {{RelationProvider}}
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val client = getBigQueryClient(parameters)
    val table = getBigQueryTableReference(sqlContext, client, parameters)

    parameters.getOrElse("type", "direct") match {
      case "direct" =>
        BigQueryTableRelation(sqlContext, client, table)

      case tpe =>
        val format = FileExportFormat(tpe)
        getTableExportFileRelation(sqlContext, client, table, format)
    }
  }

  // See {{CreatableRelationProvider}}
  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String],
                              data: DataFrame): BaseRelation = {

    val client = getBigQueryClient(parameters)
    val table = parameters.get("table").map(BigQueryTableReference(_)).getOrElse(
      throw new MissingParameterException(
        "A parameter 'table' of the form [projectId].[datasetId].[tableId] must be specified."
      )
    )
    client.writeTable(data, table, mode)
    BigQueryTableRelation(sqlContext, client, table)
  }
}
