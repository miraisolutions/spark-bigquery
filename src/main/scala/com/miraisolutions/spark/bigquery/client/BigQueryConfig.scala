package com.miraisolutions.spark.bigquery.client

import com.google.cloud.bigquery.QueryJobConfiguration.Priority

private object StagingDatasetConfig {
  private val namespace = "bq.staging_dataset."

  val DESCRIPTION = "Spark BigQuery staging dataset"

  object Keys {
    val NAME = namespace + "name"
    val LOCATION = namespace + "location"
    val LIFETIME = namespace + "lifetime"
  }

  object Defaults {
    val NAME = "spark_staging"
    val LIFETIME = 86400000L
  }
}

private[bigquery] case class StagingDatasetConfig(name: String, location: String, lifetime: Long)


private object JobConfig {
  private val namespace = "bq.job."

  object Keys {
    val PRIORITY = namespace + "priority"
  }

  object Defaults {
    val PRIORITY = Priority.INTERACTIVE
  }
}

private[bigquery] case class JobConfig(priority: Priority)


private[bigquery] object BigQueryConfig {
  private val namespace = "bq."

  private object Keys {
    val PROJECT = namespace + "project"
  }

  def apply(parameters: Map[String, String]): BigQueryConfig = {
    val project = parameters(Keys.PROJECT)

    val stagingDataset = StagingDatasetConfig(
      name = parameters.getOrElse(StagingDatasetConfig.Keys.NAME, StagingDatasetConfig.Defaults.NAME),
      location = parameters(StagingDatasetConfig.Keys.LOCATION),
      lifetime = parameters.get(StagingDatasetConfig.Keys.LIFETIME).map(_.toLong).getOrElse(StagingDatasetConfig.Defaults.LIFETIME)
    )

    val job = JobConfig(priority = parameters.get(JobConfig.Keys.PRIORITY).map(Priority.valueOf).getOrElse(JobConfig.Defaults.PRIORITY))

    BigQueryConfig(project, stagingDataset, job)
  }
}

private[bigquery] case class BigQueryConfig(project: String, stagingDataset: StagingDatasetConfig, job: JobConfig)
