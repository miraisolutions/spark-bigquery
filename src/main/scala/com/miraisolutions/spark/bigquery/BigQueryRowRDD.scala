package com.miraisolutions.spark.bigquery

import com.miraisolutions.spark.bigquery.client.BigQueryTableReader
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

/**
  * BigQuery row RDD
  * @param sc Spark context
  * @param table Table reader used to read a BigQuery table through a number of partitions
  */
class BigQueryRowRDD(sc: SparkContext, val table: BigQueryTableReader) extends RDD[Row](sc, Seq.empty) {

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    table.getRows(split.index).iterator
  }

  override protected def getPartitions: Array[Partition] = {
    (0 until table.numPartitions).map(BigQueryPartition(_)).toArray
  }
}
