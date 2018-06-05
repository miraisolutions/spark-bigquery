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

import com.google.cloud.bigquery.FormatOptions

/**
  * File format used in conjunction with Spark and BigQuery import/export.
  */
private sealed trait FileFormat {
  /** Spark format identifier */
  def sparkFormatIdentifier: String
  /** BigQuery format identifier */
  def bigQueryFormatIdentifier: String
  /** BigQuery format options */
  def bigQueryFormatOptions: FormatOptions
  /** File extension */
  def fileExtension: String
}

private object FileFormat {

  /**
    * JSON format.
    * @see https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-json
    */
  case object JSON extends FileFormat {
    override val sparkFormatIdentifier: String = "json"
    override val bigQueryFormatIdentifier: String = "NEWLINE_DELIMITED_JSON"
    override val bigQueryFormatOptions: FormatOptions = FormatOptions.json()
    override val fileExtension: String = "json"
  }

  /**
    * CSV format.
    * @see https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-csv
    */
  case object CSV extends FileFormat {
    override val sparkFormatIdentifier: String = "csv"
    override val bigQueryFormatIdentifier: String = "CSV"
    override val bigQueryFormatOptions: FormatOptions = FormatOptions.csv()
    override val fileExtension: String = "csv"
  }

  /**
    * Avro format.
    * @see https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-avro
    * @see https://github.com/databricks/spark-avro
    */
  case object AVRO extends FileFormat {
    override val sparkFormatIdentifier: String = "com.databricks.spark.avro"
    override val bigQueryFormatIdentifier: String = "AVRO"
    override val bigQueryFormatOptions: FormatOptions = FormatOptions.avro()
    override val fileExtension: String = "avro"
  }

  /**
    * Parquet format.
    * @see https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-parquet
    */
  case object PARQUET extends FileFormat {
    override val sparkFormatIdentifier: String = "parquet"
    override val bigQueryFormatIdentifier: String = "PARQUET"
    // NOTE: importing from parquet is in beta while exporting to parquet is not yet supported
    override val bigQueryFormatOptions: FormatOptions = FormatOptions.of("PARQUET")
    override val fileExtension: String = "parquet"
  }

  /** Creates a file format from a string. */
  def apply(format: String): FileFormat = {
    format.toLowerCase match {
      case "parquet" => PARQUET
      case "avro" => AVRO
      case "json" => JSON
      case "csv" => CSV
      case _ =>
        throw new IllegalArgumentException(s"Unsupported file format: $format")
    }
  }
}
