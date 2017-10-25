package com.miraisolutions.spark.bigquery

/**
  * Exception to be thrown in case of a missing parameter
  * @param message Exception message
  */
private class MissingParameterException(message: String) extends Exception(message)
