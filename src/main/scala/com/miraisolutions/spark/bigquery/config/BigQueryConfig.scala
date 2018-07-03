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

package com.miraisolutions.spark.bigquery.config

import com.google.cloud.bigquery.QueryJobConfiguration.Priority

private[bigquery] object StagingDatasetConfig {
  private val namespace = "bq.staging_dataset."

  val DESCRIPTION = "Spark BigQuery staging dataset"

  object Keys {
    val NAME = namespace + "name"
    val LIFETIME = namespace + "lifetime"
    val GCS_BUCKET = namespace + "gcs_bucket"
  }

  object Defaults {
    val NAME = "spark_staging"
    val LIFETIME = 86400000L
  }
}

/**
  * BigQuery staging dataset configuration. A staging dataset is used to temporarily store the results of SQL queries.
  * @param name Name of staging dataset
  * @param lifetime Default table lifetime in milliseconds. Tables are automatically deleted once the lifetime has
  *                 been reached.
  * @param gcsBucket Google Cloud Storage (GCS) bucket to use for storing temporary files. Temporary files are used
  *                  when importing through BigQuery load jobs and exporting through BigQuery extraction jobs.
  * @see [[https://cloud.google.com/bigquery/docs/dataset-locations]]
  */
case class StagingDatasetConfig(
  name: String = StagingDatasetConfig.Defaults.NAME,
  lifetime: Long = StagingDatasetConfig.Defaults.LIFETIME,
  gcsBucket: String
)


private[bigquery] object JobConfig {
  private val namespace = "bq.job."

  object Keys {
    val PRIORITY = namespace + "priority"
    val TIMEOUT = namespace + "timeout"
  }

  object Defaults {
    val PRIORITY = Priority.INTERACTIVE
    val TIMEOUT = 3600000L
  }
}

/**
  * BigQuery job configuration options.
  * @param priority BigQuery job priority when executing SQL queries. Defaults to "interactive", i.e. the
  *                 query is executed as soon as possible.
  * @param timeout Timeout in milliseconds after which a file import/export job should be considered as failed.
  *                Defaults to 3600000 ms = 1 h.
  * @see [[https://cloud.google.com/bigquery/quota-policy]]
  */
case class JobConfig(
  priority: Priority = JobConfig.Defaults.PRIORITY,
  timeout: Long = JobConfig.Defaults.TIMEOUT
)


private[bigquery] object BigQueryConfig {
  private val namespace = "bq."

  object Keys {
    val PROJECT = namespace + "project"
    val LOCATION = namespace + "location"
    val SERVICE_ACCOUNT_KEY_FILE = namespace + "service_account_key_file"
  }

  /**
    * Constructs typed BigQuery configuration options from a parameter map.
    * @param parameters Parameter map
    */
  def apply(parameters: Map[String, String]): BigQueryConfig = {
    val project = parameters(Keys.PROJECT)
    val location = parameters(Keys.LOCATION)
    val serviceAccountKeyFile = parameters.get(Keys.SERVICE_ACCOUNT_KEY_FILE)

    val stagingDataset = StagingDatasetConfig(
      name = parameters.getOrElse(StagingDatasetConfig.Keys.NAME, StagingDatasetConfig.Defaults.NAME),
      lifetime = parameters.get(StagingDatasetConfig.Keys.LIFETIME).map(_.toLong)
        .getOrElse(StagingDatasetConfig.Defaults.LIFETIME),
      gcsBucket = parameters(StagingDatasetConfig.Keys.GCS_BUCKET)
    )

    val job = JobConfig(
      priority = parameters.get(JobConfig.Keys.PRIORITY).map(Priority.valueOf).getOrElse(JobConfig.Defaults.PRIORITY),
      timeout = parameters.get(JobConfig.Keys.TIMEOUT).map(_.toLong).getOrElse(JobConfig.Defaults.TIMEOUT)
    )

    BigQueryConfig(project, location , serviceAccountKeyFile, stagingDataset, job)
  }
}

/**
  * BigQuery configuration.
  *
  * @param project               BigQuery billing project ID.
  * @param location              Geographic location where newly created datasets should reside. "EU" or "US".
  *                              This holds for new datasets that are being created as part of a Spark write operation
  *                              and for temporary staging datasets.
  * @param serviceAccountKeyFile Optional Google Cloud service account key file to use for authentication with Google
  *                              Cloud services. The use of service accounts is highly recommended. Specifically, the
  *                              service account will be used to interact with BigQuery and Google Cloud Storage (GCS).
  *                              If not specified, application default credentials will be used.
  * @param stagingDataset        BigQuery staging dataset configuration options.
  * @param job                   BigQuery job configuration options.
  * @see [[https://cloud.google.com/bigquery/pricing]]
  * @see [[https://cloud.google.com/bigquery/docs/dataset-locations]]
  * @see [[https://cloud.google.com/docs/authentication/]]
  * @see [[https://cloud.google.com/bigquery/docs/authentication/]]
  * @see [[https://cloud.google.com/storage/docs/authentication/]]
  */
case class BigQueryConfig(
  project: String,
  location: String,
  serviceAccountKeyFile: Option[String] = None,
  stagingDataset: StagingDatasetConfig,
  job: JobConfig = JobConfig()
)
