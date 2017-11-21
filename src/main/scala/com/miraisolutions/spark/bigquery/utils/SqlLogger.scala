package com.miraisolutions.spark.bigquery.utils

import org.slf4j.Logger

/**
  * SQL logging wrapper for logging SQL queries
  * @param logger Slf4j logger to use for logging
  */
private[bigquery] case class SqlLogger(logger: Logger) {
  def logSqlQuery(sqlQuery: String): Unit = {
    logger.info("Executing SQL query: " + sqlQuery.replaceAllLiterally("\n", ""))
  }
}
