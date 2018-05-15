package com.miraisolutions.spark.bigquery

import com.miraisolutions.spark.bigquery.client.BigQueryTableResult
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

// See also https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/rdd/JdbcRDD.scala

class BigQueryRowRDD(sc: SparkContext, val table: BigQueryTableResult) extends RDD[Row](sc, Seq.empty) {

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    table.getRows(split.index).iterator
  }

  override protected def getPartitions: Array[Partition] = {
    (0 until table.numPartitions).map(BigQueryPartition(_)).toArray
  }
}
