package com.miraisolutions.spark.bigquery

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

// See also https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/rdd/JdbcRDD.scala

class BigQueryRowRDD(sc: SparkContext, table: BigQueryTable, numPartitions: Int) extends RDD[Row](sc, Seq.empty) {

  @transient private val tableResult = BigQueryClient.getTable(table, numPartitions)

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    tableResult.getRows(split.index).iterator
  }

  override protected def getPartitions: Array[Partition] = {
    (0 until numPartitions).map(BigQueryPartition(_)).toArray
  }
}
