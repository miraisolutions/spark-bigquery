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
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.sources._

/**
  * Google BigQuery default data source
  */
class DefaultSource extends RelationProvider with CreatableRelationProvider with DataSourceRegister {

  /** Short name for data source */
  override def shortName(): String = "bigquery"

  private def getBigQueryClient(parameters: Map[String, String]): BigQueryClient = {
    new BigQueryClient(BigQueryConfig(parameters))
  }

  /**
    * Retrieves a BigQuery table relation
    * @param sqlContext Spark SQL context
    * @param parameters Connection parameters
    * @return Some BigQuery table relation if the 'table' parameter has been specified, None otherwise
    */
  private def getTableRelation(sqlContext: SQLContext,
                               parameters: Map[String, String]): Option[BigQueryTableRelation] = {
    parameters.get("table") map { ref =>
      val client = getBigQueryClient(parameters)
      new BigQueryTableRelation(sqlContext, client, BigQueryTableReference(ref))
    }
  }

  /**
    * Retrieves a BigQuery SQL relation
    * @param sqlContext Spark SQL context
    * @param parameters Connection parameters
    * @return Some BigQuery SQL relation if the 'sqlQuery' parameter has been specified, None otherwise
    */
  private def getSqlRelation(sqlContext: SQLContext,
                             parameters: Map[String, String]): Option[BigQuerySqlRelation] = {
    parameters.get("sqlQuery") map { query =>
      val client = getBigQueryClient(parameters)
      new BigQuerySqlRelation(sqlContext, client, query)
    }
  }

  // See {{RelationProvider}}
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    getTableRelation(sqlContext, parameters)
      .orElse(getSqlRelation(sqlContext, parameters))
      .getOrElse(throw new MissingParameterException(
        "Either a parameter 'table' of the form [projectId].[datasetId].[tableId] or 'sqlQuery' must be specified."
      ))
  }

  // See {{CreatableRelationProvider}}
  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String],
                              data: DataFrame): BaseRelation = {

    getTableRelation(sqlContext, parameters).fold(
      throw new MissingParameterException(
        "A parameter 'table' of the form [projectId].[datasetId].[tableId] must be specified."
      )
    ) { relation =>

      relation.client.writeTable(data, relation.table, mode)
      relation
    }
  }
}
