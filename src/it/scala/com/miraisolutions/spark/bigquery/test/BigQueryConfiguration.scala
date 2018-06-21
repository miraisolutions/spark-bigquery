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

package com.miraisolutions.spark.bigquery.test

import com.miraisolutions.spark.bigquery.BigQueryTableReference
import com.miraisolutions.spark.bigquery.config.{BigQueryConfig, _}
import org.apache.spark.sql.{DataFrameReader, DataFrameWriter, Row}
import org.scalatest.{Outcome, TestSuite, TestSuiteMixin}

private object BigQueryConfiguration {
  // BigQuery test dataset name
  private val BIGQUERY_TEST_DATASET = "spark_bigquery_test"
}

/**
  * BigQuery configuration test suite mixin. Tests mixing in that trait can be run in sbt via:
  *
  * it:testOnly com.miraisolutions.spark.bigquery.* --
  * -Dbq.project=<project>
  * -Dbq.location=<location>
  * -Dbq.staging_dataset.gcs_bucket=<gcs_bucket>
  * -Dbq.staging_dataset.service_account_key_file=<path_to_keyfile>
  */
private[bigquery] trait BigQueryConfiguration extends TestSuiteMixin { this: TestSuite =>
  import BigQueryConfiguration._

  // Captured BigQuery test configuration
  var config: BigQueryConfig = _

  /**
    * Construct a table reference to a table in the configured BigQuery test dataset.
    * @param table Table name
    * @return BigQuery table reference
    */
  protected def getTestDatasetTableReference(table: String): BigQueryTableReference = {
    BigQueryTableReference(config.project, BIGQUERY_TEST_DATASET, table)
  }

  /**
    * Gets the unquoted table identifier for a table in the configured BigQuery test dataset.
    * @param table Table name
    * @return Unquoted table identifier
    */
  protected def getTestDatasetTableIdentifier(table: String): String = {
    getTestDatasetTableReference(table).unquotedIdentifier
  }

  protected implicit class DataFrameReaderTestConfig(val reader: DataFrameReader) {
    /**
      * Applies BigQuery test configuration options derived from parameters passed to the test.
      * @param table BigQuery table to read
      * @param importType Import type (e.g. "direct", "parquet", "avro", ...)
      * @return Spark [[DataFrameReader]]
      */
    def bigqueryTest(table: String, importType: String = "direct"): DataFrameReader = {
      applyDataFrameOptions(reader, config)
        .option("table", getTestDatasetTableIdentifier(table))
        .option("type", importType)
    }
  }

  protected implicit class DataFrameWriterTestConfig(val writer: DataFrameWriter[Row]) {
    /**
      * Applies BigQuery test configuration options derived from parameters passed to the test.
      * @param table BigQuery table to write
      * @param exportType Export type (e.g. "direct", "parquet", "avro", ...)
      * @return Spark [[DataFrameWriter]]
      */
    def bigqueryTest(table: String, exportType: String = "direct"): DataFrameWriter[Row] = {
      applyDataFrameOptions(writer, config)
        .option("table", getTestDatasetTableIdentifier(table))
        .option("type", exportType)
    }
  }

  // See {{TestSuiteMixin}}
  abstract override def withFixture(test: NoArgTest): Outcome = {
    // Extract BigQuery configuration from config map
    config = BigQueryConfig(test.configMap.mapValues(_.toString))
    super.withFixture(test)
  }
}
