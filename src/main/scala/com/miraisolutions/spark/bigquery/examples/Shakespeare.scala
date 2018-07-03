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

package com.miraisolutions.spark.bigquery.examples

import org.apache.spark.sql.SparkSession
import com.miraisolutions.spark.bigquery.config._

/**
  * Reads the public Google BigQuery sample dataset 'shakespeare'.
  *
  * To run this example first compile an assembly using `sbt assembly`. Then run:
  *
  * ==Local Spark Cluster==
  * `spark-submit --class com.miraisolutions.spark.bigquery.examples.Shakespeare --master local[*]
  * target/scala-2.11/spark-bigquery-assembly-<version>.jar <arguments>`
  *
  * ==Google Cloud Dataproc==
  * `gcloud dataproc jobs submit spark --cluster <cluster> --class
  * com.miraisolutions.spark.bigquery.examples.Shakespeare --jars
  * target/scala-2.11/spark-bigquery-assembly-<version>.jar -- <argument>`
  *
  * Where `<arguments>` are:
  *  1. Google BigQuery billing project ID
  *  1. Google BigQuery dataset location (EU, US)
  *  1. Google Cloud Storage (GCS) bucket where staging files will be located
  *  1. Google Cloud service account key file (required when running outside of Google Cloud)
  *
  * @see [[https://cloud.google.com/bigquery/public-data/]]
  * @see [[https://cloud.google.com/bigquery/docs/dataset-locations]]
  * @see [[https://cloud.google.com/storage/docs/authentication#service_accounts]]
  * @see [[https://cloud.google.com/dataproc/]]
  */
object Shakespeare {
  def main(args: Array[String]): Unit = {

    // Initialize Spark session
    val spark = SparkSession
      .builder
      .appName("Google BigQuery Shakespeare")
      .getOrCreate

    import spark.implicits._

    // Define BigQuery options
    val config = BigQueryConfig(
      project = args(0), // Google BigQuery billing project ID
      location = args(1), // Google BigQuery dataset location
      stagingDataset = StagingDatasetConfig(
        gcsBucket = args(2) // Google Cloud Storage bucket for staging files
      ),
      serviceAccountKeyFile = if(args.length > 3) Some(args(3)) else None // Google Cloud service account key file
    )

    // Read public shakespeare data table using direct import (streaming)
    val shakespeare = spark.read
      .bigquery(config)
      .option("table", "bigquery-public-data.samples.shakespeare")
      .option("type", "direct")
      .load()

    val hamlet = shakespeare.filter($"corpus".like("hamlet"))
    hamlet.show(100)

    shakespeare.createOrReplaceTempView("shakespeare")
    val macbeth = spark.sql("SELECT * FROM shakespeare WHERE corpus = 'macbeth'").persist()
    macbeth.show(100)

    // Write filtered data table via a Parquet export on GCS
    macbeth.write
      .bigquery(config)
      .option("table", args(0) + ".samples.macbeth")
      .option("type", "parquet")
      .save()
  }
}
