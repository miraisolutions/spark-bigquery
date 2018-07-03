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

import com.google.cloud.bigquery.BigQuery.TableDataListOption
import com.google.cloud.bigquery._
import org.slf4j.LoggerFactory

package object client {

  private val logger = LoggerFactory.getLogger(this.getClass.getName)

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
      logger.info(s"Creating table $table in dataset ${ds.getDatasetId.getDataset} " +
        s"of project ${ds.getDatasetId.getProject}")

      val tableDefinition = StandardTableDefinition.newBuilder()
        .setType(TableDefinition.Type.TABLE)
        .setSchema(schema)
        .build()

      ds.create(table, tableDefinition)
    }

    def dropTable(table: String): Unit = {
      fold(table)((): Unit) { table =>
        logger.info(s"Deleting table $table in dataset ${ds.getDatasetId.getDataset} " +
          s"of project ${ds.getDatasetId.getProject}")
        table.delete()
      }
    }

    def dropAndCreateTable(table: String, schema: Schema): Table = {
      dropTable(table)
      createTable(table, schema)
    }
  }

}
