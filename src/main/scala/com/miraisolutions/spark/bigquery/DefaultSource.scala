package com.miraisolutions.spark.bigquery

import com.google.cloud.hadoop.io.bigquery.BigQueryStrings
import com.spotify.spark.bigquery._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources._

class DefaultSource extends RelationProvider {
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    parameters.get("bq.project.id").foreach(sqlContext.setBigQueryProjectId)
    parameters.get("bq.gcs.bucket").foreach(sqlContext.setBigQueryGcsBucket)
    parameters.get("bq.dataset.location").foreach(sqlContext.setBigQueryDatasetLocation)

    parameters.get("tableReference").fold(
      throw new MissingParameterException(
        "The required parameter 'tableReference' of the form [projectId]:[datasetId].[tableId] was not specified."
      )
    ) { tableReference =>
      val ref = BigQueryStrings.parseTableReference(tableReference)
      BigQuerySourceRelation(ref, sqlContext)
    }
  }
}
