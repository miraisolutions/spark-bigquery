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

/**
  * Reads the public Google BigQuery sample dataset 'shakespeare'.
  * See [[https://cloud.google.com/bigquery/public-data/]].
  *
  * Run by providing:
  *  1. Google BigQuery billing project ID
  *  1. Google BigQuery staging dataset location (EU, US)
  */
object Shakespeare {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Google BigQuery Shakespeare")
      .getOrCreate

    val shakespeare = spark.read
      .format("bigquery")
      .option("bq.project", args(0))
      .option("bq.staging_dataset.location", args(1))
      .option("table", "bigquery-public-data.samples.shakespeare")
      .load()

    import spark.implicits._

    val hamlet = shakespeare.filter($"corpus".like("hamlet"))
    hamlet.show(100)

    shakespeare.createOrReplaceTempView("shakespeare")
    val macbeth = spark.sql("SELECT * FROM shakespeare WHERE corpus = 'macbeth'")
    macbeth.show(100)
  }
}
