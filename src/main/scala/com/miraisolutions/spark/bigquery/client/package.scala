package com.miraisolutions.spark.bigquery

import com.google.cloud.bigquery.BigQuery.{DatasetDeleteOption, TableDataListOption}
import com.google.cloud.bigquery._

package object client {

  private[client] implicit class BigQueryDataset(val ds: Dataset) extends AnyVal {

    def getOrCreateTable(table: String, schema: Schema): Table = {
      val tbl = ds.get(table)
      if(tbl.exists()) tbl else createTable(table, schema)
    }

    def existsTable(table: String): Boolean = {
      ds.get(table).exists()
    }

    def isNonEmptyTable(table: String): Boolean = {
      ds.get(table).list(TableDataListOption.pageSize(1)).getTotalRows > 0
    }

    def existsNonEmptyTable(table: String): Boolean = {
      existsTable(table) && isNonEmptyTable(table)
    }

    def createTable(table: String, schema: Schema): Table = {
      val tableDefinition = StandardTableDefinition.newBuilder()
        .setType(TableDefinition.Type.TABLE)
        .setSchema(schema)
        .build()

      ds.create(table, tableDefinition)
    }

    def dropTable(table: String): Unit = {
      if(ds.exists()) ds.delete(DatasetDeleteOption.deleteContents())
    }

    def dropAndCreateTable(table: String, schema: Schema): Table = {
      dropTable(table)
      createTable(table, schema)
    }
  }

}
