package com.miraisolutions.spark.bigquery

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.miraisolutions.spark.bigquery.client.BigQueryConfig
import org.apache.spark.sql.{DataFrameReader, DataFrameWriter, Row}
import org.scalatest.{Outcome, TestSuite, TestSuiteMixin}

/**
  * BigQuery configuration test suite mixin.
  */
trait BigQueryConfiguration extends TestSuiteMixin { this: TestSuite with DataFrameSuiteBase =>

  private var config: BigQueryConfig = _

  protected implicit class DataFrameReaderConfig(val reader: DataFrameReader) {
    def withBigQueryTestOptions(dataset: String = "spark_bigquery_test", table: String): DataFrameReader = {
      reader
        .format("bigquery")
        .option("bq.project", config.project)
        .option("bq.staging_dataset.location", config.stagingDataset.location)
        .option("bq.staging_dataset.gcs_bucket", config.stagingDataset.gcsBucket)
        .option("table", s"${config.project}.$dataset.$table")
    }
  }

  protected implicit class DataFrameWriterConfig(val writer: DataFrameWriter[Row]) {
    def withBigQueryTestOptions(dataset: String = "spark_bigquery_test", table: String): DataFrameWriter[Row] = {
      writer
        .format("bigquery")
        .option("bq.project", config.project)
        .option("bq.staging_dataset.location", config.stagingDataset.location)
        .option("bq.staging_dataset.gcs_bucket", config.stagingDataset.gcsBucket)
        .option("table", s"${config.project}.$dataset.$table")
    }
  }

  abstract override def withFixture(test: NoArgTest): Outcome = {
    config = BigQueryConfig(test.configMap.mapValues(_.toString))
    val svcAccountKeyFile = test.configMap.getRequired[String]("svcAccountKeyFile")

    val hconf = sc.hadoopConfiguration
    hconf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    hconf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    hconf.set("fs.gs.project.id", config.project)
    hconf.set("google.cloud.auth.service.account.enable", "true")
    hconf.set("google.cloud.auth.service.account.json.keyfile", svcAccountKeyFile)

    super.withFixture(test)
  }
}
