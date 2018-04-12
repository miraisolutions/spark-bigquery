package com.miraisolutions.spark.bigquery

import org.apache.spark.Partition

/**
  * BigQuery table partition identifier
  * @param index Partition index
  */
private final case class BigQueryPartition(override val index: Int) extends Partition
