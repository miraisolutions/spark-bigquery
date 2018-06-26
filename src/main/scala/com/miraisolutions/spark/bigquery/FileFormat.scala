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
  /** BigQuery format options */
  def bigQueryFormatOptions: FormatOptions
  /** File extension */
  def fileExtension: String
}

private object FileFormat {

  /**
    * JSON format.
    * @see [[https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-json]]
    */
  case object JSON extends FileFormat {
    override val sparkFormatIdentifier: String = "json"
    override val bigQueryFormatOptions: FormatOptions = FormatOptions.json()
    override val fileExtension: String = "json"
  }

  /**
    * CSV format.
    * @see [[https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-csv]]
    */
  case object CSV extends FileFormat {
    override val sparkFormatIdentifier: String = "csv"
    override val bigQueryFormatOptions: FormatOptions = FormatOptions.csv()
    override val fileExtension: String = "csv"
  }

  /**
    * Avro format.
    * @see [[https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-avro]]
    * @see [[https://github.com/databricks/spark-avro]]
    */
  case object AVRO extends FileFormat {
    override val sparkFormatIdentifier: String = "com.databricks.spark.avro"
    override val bigQueryFormatOptions: FormatOptions = FormatOptions.avro()
    override val fileExtension: String = "avro"
  }

  /**
    * Parquet format.
    * @see [[https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-parquet]]
    * @see [[https://spark.apache.org/docs/latest/sql-programming-guide.html#parquet-files]]
    */
  case object PARQUET extends FileFormat {
    override val sparkFormatIdentifier: String = "parquet"
    override val bigQueryFormatOptions: FormatOptions = FormatOptions.parquet()
    override val fileExtension: String = "parquet"
  }

  /**
    * ORC format.
    * @see [[https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-orc]]
    * @see [[https://spark.apache.org/docs/latest/sql-programming-guide.html#orc-files]]
    */
  case object ORC extends FileFormat {
    override val sparkFormatIdentifier: String = "orc"
    override val bigQueryFormatOptions: FormatOptions = FormatOptions.orc()
    override val fileExtension: String = "orc"
  }

  /** Creates a file format from a string. */
  def apply(format: String): FileFormat = {
    format.toLowerCase match {
      case "parquet" => PARQUET
      case "avro" => AVRO
      case "orc" => ORC
      case "json" => JSON
      case "csv" => CSV
      case _ =>
        throw new IllegalArgumentException(s"Unsupported file format: $format")
    }
  }
}
