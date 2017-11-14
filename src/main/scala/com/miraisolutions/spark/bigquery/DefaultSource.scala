package com.miraisolutions.spark.bigquery

import com.google.cloud.hadoop.io.bigquery.BigQueryStrings
import com.spotify.spark.bigquery._
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.sources._

/**
  * Google BigQuery default data source
  */
class DefaultSource extends RelationProvider with CreatableRelationProvider with DataSourceRegister {

  /** Short name for data source */
  override def shortName(): String = "bigquery"

  /**
    * Sets BigQuery connection parameters on the Spark {{SQLContext}}
    * @param sqlContext Spark SQL context
    * @param parameters Connection parameters
    */
  protected def setBigQueryContext(sqlContext: SQLContext, parameters: Map[String, String]): Unit = {
    parameters foreach {
      case ("bq.project.id", value) =>
        sqlContext.setBigQueryProjectId(value)
      case ("bq.gcs.bucket", value) =>
        sqlContext.setBigQueryGcsBucket(value)
      case ("bq.dataset.location", value) =>
        sqlContext.setBigQueryDatasetLocation(value)
      case (key, value) =>
        sqlContext.sparkContext.hadoopConfiguration.set(key, value)
    }
  }

  /**
    * Retrieves a BigQuery table relation
    * @param sqlContext Spark SQL context
    * @param parameters Connection parameters
    * @return Some BigQuery table relation if the 'table' parameter has been specified, None otherwise
    */
  private def getTableRelation(sqlContext: SQLContext,
                                 parameters: Map[String, String]): Option[BigQueryTableRelation] = {
    parameters
      .get("table")
      .map(ref => BigQueryTableRelation(BigQueryStrings.parseTableReference(ref), sqlContext))
  }

  /**
    * Retrieves a BigQuery SQL relation
    * @param sqlContext Spark SQL context
    * @param parameters Connection parameters
    * @return Some BigQuery SQL relation if the 'sqlQuery' parameter has been specified, None otherwise
    */
  private def getSqlRelation(sqlContext: SQLContext,
                               parameters: Map[String, String]): Option[BigQuerySqlRelation] = {
    parameters
      .get("sqlQuery")
      .map(query => BigQuerySqlRelation(query, sqlContext))
  }

  // See {{RelationProvider}}
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    setBigQueryContext(sqlContext, parameters)

    getTableRelation(sqlContext, parameters)
      .orElse(getSqlRelation(sqlContext, parameters))
      .getOrElse(throw new MissingParameterException(
        "Either a parameter 'table' of the form [projectId]:[datasetId].[tableId] or 'sqlQuery' must be specified."
      ))
  }

  // See {{CreatableRelationProvider}}
  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String],
                              data: DataFrame): BaseRelation = {
    import SaveMode._
    import WriteDisposition._
    import CreateDisposition._

    setBigQueryContext(sqlContext, parameters)

    getTableRelation(sqlContext, parameters).fold(
      throw new MissingParameterException(
        "A parameter 'table' of the form [projectId]:[datasetId].[tableId] must be specified."
      )
    ) { relation =>

      mode match {
        case Append =>
          data.saveAsBigQueryTable(relation.tableRef, WRITE_APPEND, CREATE_IF_NEEDED)

        case Overwrite =>
          data.saveAsBigQueryTable(relation.tableRef, WRITE_TRUNCATE, CREATE_IF_NEEDED)

        case ErrorIfExists =>
          data.saveAsBigQueryTable(relation.tableRef, WRITE_EMPTY, CREATE_IF_NEEDED)

        case Ignore =>
          try {
            data.saveAsBigQueryTable(relation.tableRef, WRITE_EMPTY, CREATE_IF_NEEDED)
          } catch {
            case _: Throwable => // ignore
          }
      }

      relation
    }
  }
}
