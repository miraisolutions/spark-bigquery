package com.miraisolutions.spark.bigquery.exception

/**
  * Signals an error when trying to read from or write to BigQuery.
  * @param message Exception message
  */
private[bigquery] class IOException(message: String) extends java.io.IOException(message)
