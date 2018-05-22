package com.miraisolutions.spark.bigquery.exception

/**
  * Signals that an error occurred while parsing a string.
  * @param message Exception message
  */
private[bigquery] class ParseException(message: String) extends Exception(message)
