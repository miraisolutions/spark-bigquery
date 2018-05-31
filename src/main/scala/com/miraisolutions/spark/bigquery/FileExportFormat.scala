package com.miraisolutions.spark.bigquery

/**
  * File export format used in conjunction with BigQuery exports.
  */
private sealed trait FileExportFormat {
  /** Spark format identifier */
  def sparkFormatIdentifier: String
  /** BigQuery format identifier */
  def bigQueryFormatIdentifier: String
  /** File extension */
  def fileExtension: String
}

private object FileExportFormat {

  case object JSON extends FileExportFormat {
    override val sparkFormatIdentifier: String = "json"
    override val bigQueryFormatIdentifier: String = "NEWLINE_DELIMITED_JSON"
    override val fileExtension: String = "json"
  }

  case object CSV extends FileExportFormat {
    override val sparkFormatIdentifier: String = "csv"
    override val bigQueryFormatIdentifier: String = "CSV"
    override val fileExtension: String = "csv"
  }

  /** Creates a file export format from a string. */
  def apply(format: String): FileExportFormat = {
    format.toLowerCase match {
      case "json" => JSON
      case "csv" => CSV
      case _ =>
        throw new IllegalArgumentException(s"Unsupported export format: $format")
    }
  }
}
