/*
 * Copyright (c) 2018 Mirai Solutions GmbH
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.miraisolutions.spark.bigquery.client

import com.google.cloud.bigquery.QueryJobConfiguration.Priority

private object StagingDatasetConfig {
  private val namespace = "bq.staging_dataset."

  val DESCRIPTION = "Spark BigQuery staging dataset"

  object Keys {
    val NAME = namespace + "name"
    val LOCATION = namespace + "location"
    val LIFETIME = namespace + "lifetime"
    val GCS_BUCKET = namespace + "gcs_bucket"
  }

  object Defaults {
    val NAME = "spark_staging"
    val LIFETIME = 86400000L
  }
}

private[bigquery] case class StagingDatasetConfig(name: String, location: String, lifetime: Long, gcsBucket: String)


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
      lifetime = parameters.get(StagingDatasetConfig.Keys.LIFETIME).map(_.toLong).getOrElse(StagingDatasetConfig.Defaults.LIFETIME),
      gcsBucket = parameters(StagingDatasetConfig.Keys.GCS_BUCKET)
    )

    val job = JobConfig(priority = parameters.get(JobConfig.Keys.PRIORITY).map(Priority.valueOf).getOrElse(JobConfig.Defaults.PRIORITY))

    BigQueryConfig(project, stagingDataset, job)
  }
}

private[bigquery] case class BigQueryConfig(project: String, stagingDataset: StagingDatasetConfig, job: JobConfig)
