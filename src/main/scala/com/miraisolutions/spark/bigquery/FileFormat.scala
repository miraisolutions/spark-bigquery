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

  case object JSON extends FileFormat {
    override val sparkFormatIdentifier: String = "json"
    override val bigQueryFormatIdentifier: String = "NEWLINE_DELIMITED_JSON"
    override val bigQueryFormatOptions: FormatOptions = FormatOptions.json()
    override val fileExtension: String = "json"
  }

  case object CSV extends FileFormat {
    override val sparkFormatIdentifier: String = "csv"
    override val bigQueryFormatIdentifier: String = "CSV"
    override val bigQueryFormatOptions: FormatOptions = FormatOptions.csv()
    override val fileExtension: String = "csv"
  }

  /** Creates a file format from a string. */
  def apply(format: String): FileFormat = {
    format.toLowerCase match {
      case "json" => JSON
      case "csv" => CSV
      case _ =>
        throw new IllegalArgumentException(s"Unsupported file format: $format")
    }
  }
}
