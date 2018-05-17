package com.miraisolutions.spark.bigquery

import com.google.cloud.bigquery.BigQuery.TableDataListOption
import com.google.cloud.bigquery._

package object client {

  /**
    * Some convenience methods on [[Dataset]]
    * @param ds BigQuery [[Dataset]]
    */
  private[client] implicit class BigQueryDataset(val ds: Dataset) {

    private def fold[T](table: String)(ifNotExists: => T)(f: Table => T): T = {
      Option(ds.get(table)).fold(ifNotExists) { tbl =>
        if(tbl.exists()) f(tbl) else ifNotExists
      }
    }

    def getOrCreateTable(table: String, schema: Schema): Table = {
      fold(table)(createTable(table, schema))(identity)
    }

    def existsTable(table: String): Boolean = {
      fold(table)(false)(_ => true)
    }

    def isNonEmptyTable(table: String): Boolean = {
      fold(table)(false)(_.list(TableDataListOption.pageSize(1)).getTotalRows > 0)
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
      fold(table)((): Unit)(_.delete())
    }

    def dropAndCreateTable(table: String, schema: Schema): Table = {
      dropTable(table)
      createTable(table, schema)
    }
  }

}
