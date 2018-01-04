# spark-bigquery

This project provides a Google BigQuery data source (`com.miraisolutions.spark.bigquery.DefaultSource`) to Spark on top of [Spotify's spark-bigquery library](https://github.com/spotify/spark-bigquery).

This data source is used in the [sparkbq](https://github.com/miraisolutions/sparkbq) R package.

## Building

Due to dependency version mismatches between Apache Spark and Google client libraries (e.g. Google Guava) this project uses [`sbt-assembly`](https://github.com/sbt/sbt-assembly) to build a fat JAR using [shading](https://github.com/sbt/sbt-assembly#shading) to relocate relevant Google classes.

ProGuard may be used via the [`sbt-proguard`](https://github.com/sbt/sbt-proguard) plugin to further shrink the assembly size. Simply run `sbt proguard` to perform that step. The resulting JAR file can be found in the `target/proguard` folder.

## Version Information

The following table provides an overview over supported versions of Spark, Scala and [Google Dataproc](https://cloud.google.com/dataproc/docs/concepts/versioning/dataproc-versions):

| spark-bigquery | Spark | Scala | Google Dataproc |
| :-----: | ----- | ----- | --------------- |
| 0.1.x | 2.2 | 2.11 | 1.2 |

## Example

The provided Google BigQuery data source (`com.miraisolutions.spark.bigquery.DefaultSource`) can be used as follows:

``` scala
val shakespeare = spark.read
  .format("bigquery")
  .option("bq.project.id", "<your_billing_project_id>")
  .option("bq.gcs.bucket", "<your_gcs_bucket>")
  .option("table", "bigquery-public-data:samples.shakespeare")
  .load()

import spark.implicits._

val hamlet = shakespeare.filter($"corpus".like("hamlet"))
hamlet.show(100)

shakespeare.createOrReplaceTempView("shakespeare")
val macbeth = spark.sql("SELECT * FROM shakespeare WHERE corpus = 'macbeth'")
macbeth.show(100)
```

You can find a complete example at `com.miraisolutions.spark.bigquery.examples.Shakespeare`.

To run this example on Google Dataproc (assuming you have the [Google Cloud SDK](https://cloud.google.com/sdk/) installed):
1. Build an assembly using `sbt clean compile assembly`
2. `gcloud dataproc jobs submit spark --cluster <your_cluster_name> --class com.miraisolutions.spark.bigquery.examples.Shakespeare --jars target/scala-2.11/spark-bigquery-assembly-<version>.jar -- <your_billing_project_id> <your_gcs_bucket>`

If you are running on a Spark cluster outside of the Google Cloud, you may need to set [Google application default credentials](https://developers.google.com/identity/protocols/application-default-credentials) or relevant Spark/Hadoop configuration options. The following is an example using a local Spark cluster:

```
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/gcp_json_keyfile.json
spark-submit --class com.miraisolutions.spark.bigquery.examples.Shakespeare --master local[2] --conf spark.hadoop.google.cloud.auth.service.account.json.keyfile="/path/to/your/gcp_json_keyfile.json" target/scala-2.11/spark-bigquery-assembly-<version>.jar <your_billing_project_id> <your_gcs_bucket>
```


## License

MIT License

Copyright (c) 2018 Mirai Solutions GmbH

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
