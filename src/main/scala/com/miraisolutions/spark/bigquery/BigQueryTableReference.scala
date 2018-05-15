package com.miraisolutions.spark.bigquery

import com.google.cloud.bigquery.TableId

/**
  * BigQuery table reference
  * @param project Project ID
  * @param dataset Dataset ID
  * @param table Table ID
  */
private case class BigQueryTableReference(project: String, dataset: String, table: String) {
  /** BigQuery Standard SQL table identifier (quoted) */
  override def toString: String = s"`$project.$dataset.$table`"
}

private object BigQueryTableReference {

  def apply(tableId: TableId): BigQueryTableReference =
    BigQueryTableReference(tableId.getProject, tableId.getDataset, tableId.getTable)

  def apply(tableRef: String): BigQueryTableReference = {
    val Array(project, dataset, table) = tableRef.replace("`", "").split("\\.")
    BigQueryTableReference(project, dataset, table)
  }

  // Converts an internal BigQuery table reference to a Google BigQuery API `TableId`
  implicit def bigQueryTableToTableId(table: BigQueryTableReference): TableId = {
    TableId.of(table.project, table.dataset, table.table)
  }

  // Converts a Google BigQuery API `TableId` to an internal BigQuery table reference
  implicit def tableIdToBigQueryTable(tableId: TableId): BigQueryTableReference = apply(tableId)
}
