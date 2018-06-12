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

import org.apache.spark.sql.{DataFrameReader, DataFrameWriter, Row}
import scala.language.reflectiveCalls

package object config {

  // Structural helper type
  private type OPT[T] = {
    def format(source: String): T
    def option(key: String, value: String): T
  }

  // Applies format and configuration options on a DataFrameReader or DataFrameWriter
  private[bigquery] def applyDataFrameOptions[T <: OPT[T]](obj: T, config: BigQueryConfig): T = {
    val objWithOptions =
      obj
        .format(DefaultSource.BIGQUERY_DATA_SOURCE_NAME)
        .option(BigQueryConfig.Keys.PROJECT, config.project)
        .option(StagingDatasetConfig.Keys.NAME, config.stagingDataset.name)
        .option(StagingDatasetConfig.Keys.LOCATION, config.stagingDataset.location)
        .option(StagingDatasetConfig.Keys.LIFETIME, config.stagingDataset.lifetime.toString)
        .option(StagingDatasetConfig.Keys.GCS_BUCKET, config.stagingDataset.gcsBucket)
        .option(JobConfig.Keys.PRIORITY, config.job.priority.toString)

    config.stagingDataset.serviceAccountKeyFile.fold(objWithOptions) { file =>
      objWithOptions.option(StagingDatasetConfig.Keys.SERVICE_ACCOUNT_KEY_FILE, file)
    }
  }

  implicit class DataFrameReaderConfig(val reader: DataFrameReader) extends AnyVal {
    /**
      * Utility method to apply typed BigQuery configuration.
      * @param config BigQuery configuration
      */
    def bigquery(config: BigQueryConfig): DataFrameReader = applyDataFrameOptions(reader, config)
  }

  implicit class DataFrameWriterConfig(val writer: DataFrameWriter[Row]) extends AnyVal {
    /**
      * Utility method to apply typed BigQuery configuration.
      * @param config BigQuery configuration
      */
    def bigquery(config: BigQueryConfig): DataFrameWriter[Row] = applyDataFrameOptions(writer, config)
  }
}
