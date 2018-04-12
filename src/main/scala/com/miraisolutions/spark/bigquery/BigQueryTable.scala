package com.miraisolutions.spark.bigquery

import com.google.cloud.bigquery.TableId

/**
  * BigQuery table reference
  * @param project Project ID
  * @param dataset Dataset ID
  * @param table Table ID
  */
private case class BigQueryTable(project: String, dataset: String, table: String) {
  /** BigQuery Standard SQL table identifier (quoted) */
  def identifier: String = s"`$project.$dataset.$table`"
}

private object BigQueryTable {
  // Converts an internal BigQuery table reference to a Google BigQuery API `TableId`
  implicit def bigQueryTableToTableId(table: BigQueryTable): TableId = {
    TableId.of(table.project, table.dataset, table.table)
  }
}
