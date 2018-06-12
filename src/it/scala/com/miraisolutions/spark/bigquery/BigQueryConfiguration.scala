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

import com.miraisolutions.spark.bigquery.config._
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.miraisolutions.spark.bigquery.config.BigQueryConfig
import org.apache.spark.sql.{DataFrameReader, DataFrameWriter, Row}
import org.scalatest.{Outcome, TestSuite, TestSuiteMixin}

private object BigQueryConfiguration {
  // BigQuery test dataset name
  private val BIGQUERY_TEST_DATASET = "spark_bigquery_test"
}

/**
  * BigQuery configuration test suite mixin.
  */
trait BigQueryConfiguration extends TestSuiteMixin { this: TestSuite with DataFrameSuiteBase =>
  import BigQueryConfiguration._

  private var config: BigQueryConfig = _

  protected implicit class DataFrameReaderTestConfig(val reader: DataFrameReader) {
    def bigqueryTest(table: String, importType: String = "direct"): DataFrameReader = {
      applyDataFrameOptions(reader, config)
        .option("table", s"${config.project}.$BIGQUERY_TEST_DATASET.$table")
        .option("type", importType)
    }
  }

  protected implicit class DataFrameWriterTestConfig(val writer: DataFrameWriter[Row]) {
    def bigqueryTest(table: String, exportType: String = "direct"): DataFrameWriter[Row] = {
      applyDataFrameOptions(writer, config)
        .option("table", s"${config.project}.$BIGQUERY_TEST_DATASET.$table")
        .option("type", exportType)
    }
  }

  abstract override def withFixture(test: NoArgTest): Outcome = {
    config = BigQueryConfig(test.configMap.mapValues(_.toString))
    super.withFixture(test)
  }
}
