# spark-bigquery: A Google BigQuery Data Source for Apache Spark

This project provides a [Google BigQuery](https://cloud.google.com/bigquery/) data source (`com.miraisolutions.spark.bigquery.DefaultSource`) to [Apache Spark](https://spark.apache.org/) using the new [Google Cloud client libraries](https://cloud.google.com/bigquery/docs/reference/libraries) for the Google BigQuery API. It supports "direct" import/export where records are directly streamed from/to BigQuery. In addition, data may be imported/exported via intermediate data extracts on [Google Cloud Storage](https://cloud.google.com/storage/) (GCS). Note that when using "direct" (streaming) export, data may not be immediately available for further querying/processing in BigQuery. It may take several minutes for streamed records to become "available". See the following resources for more information:

* https://cloud.google.com/bigquery/streaming-data-into-bigquery
* https://cloud.google.com/blog/big-data/2017/06/life-of-a-bigquery-streaming-insert

The following import/export combinations are currently supported:

|                                        | Direct             | Parquet            | Avro               | ORC                | JSON               | CSV                |
| -------------------------------------- | ------------------ | ------------------ | ------------------ | ------------------ | ------------------ | ------------------ |
| Import to Spark (export from BigQuery) | :heavy_check_mark: | :x:                | :heavy_check_mark: | :x:                | :heavy_check_mark: | :heavy_check_mark: |
| Export from Spark (import to BigQuery) | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | :x:                | :x:                |


More information on the various supported formats can be found at:

* Parquet: https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-parquet
* Avro: https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-avro
* ORC: https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-orc
* JSON: https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-json
* CSV: https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-csv

CSV and JSON are not recommended as data exchange formats between Apache Spark and BigQuery due to their lack of type safety. Better options are direct import/export, Parquet, Avro and ORC.


This data source is used in the [sparkbq](https://github.com/miraisolutions/sparkbq) R package.

## Building

Due to dependency version mismatches between Apache Spark and Google client libraries (e.g. Google Guava) this project uses [`sbt-assembly`](https://github.com/sbt/sbt-assembly) to build a fat JAR using [shading](https://github.com/sbt/sbt-assembly#shading) to relocate relevant Google classes.

## Version Information

The following table provides an overview over supported versions of Apache Spark, Scala and [Google Dataproc](https://cloud.google.com/dataproc/docs/concepts/versioning/dataproc-versions):

| spark-bigquery | Spark | Scala | Google Dataproc |
| :------------: | ----- | ----- | --------------- |
| 0.1.x          | 2.3.x | 2.11  | 1.3             |

## Example

The provided Google BigQuery data source (`com.miraisolutions.spark.bigquery.DefaultSource`) can be used as follows:

``` scala
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.miraisolutions.spark.bigquery.config._

// Initialize Spark session
val spark = SparkSession
  .builder
  .appName("Google BigQuery Shakespeare")
  .getOrCreate

import spark.implicits._

// Define BigQuery options
val config = BigQueryConfig(
  project = "<billing_project_id>",  // Google BigQuery billing project ID
  location = "<dataset_location>", // Google BigQuery dataset location
  stagingDataset = StagingDatasetConfig(
	gcsBucket = "<gcs_bucket>" // Google Cloud Storage bucket for staging files
  ),
  serviceAccountKeyFile = Some("<service_account_key_file>") // Google Cloud service account key file
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
  .option("table", "<billing_project_id>.samples.macbeth")
  .option("type", "parquet")
  .mode(SaveMode.Overwrite)
  .save()
```

You can find a complete example at `com.miraisolutions.spark.bigquery.examples.Shakespeare`.

To run this example first compile and assembly using `sbt assembly`. Then run:

**Local Spark Cluster**

`spark-submit --class com.miraisolutions.spark.bigquery.examples.Shakespeare --master local[*] target/scala-2.11/spark-bigquery-assembly-<version>.jar <arguments>`

**[Google Cloud Dataproc](https://cloud.google.com/dataproc/)**

`gcloud dataproc jobs submit spark --cluster <cluster> --class com.miraisolutions.spark.bigquery.examples.Shakespeare --jars target/scala-2.11/spark-bigquery-assembly-<version>.jar -- <argument>`

where `<arguments>` are:
1. Google BigQuery billing project ID
2. Google BigQuery dataset location (EU, US)
3. Google Cloud Storage (GCS) bucket where staging files will be located
4. Google Cloud service account key file (required when running outside of Google Cloud)


## Configuration

The three main Spark read/write options include:

* `table`: The BigQuery table to read/write. To be specified in the form `[projectId].[datasetId].[tableId]`. One of `table` or `sqlQuery` must be specified.
* `sqlQuery`: A SQL query in Google BigQuery standard SQL dialect (SQL-2011). One of `table` or `sqlQuery` must be specified. 
* `type` (optional): The BigQuery import/export type to use. Options include "direct", "parquet", "avro", "orc", "json" and "csv". Defaults to "direct". See the table at the top for supported type and import/export combinations.


In addition, there are a number of BigQuery configuration options that can be specified in two ways: the traditional way using Spark's read/write options (e.g. `spark.read.option("bq.project", "<your_project>")`) and using the `bigquery` extension method (`spark.read.bigquery(config)`; see example above) which is usually more straightforward to use. If you prefer the traditional way or if you are using spark-bigquery in a non-Scala environment (e.g. PySpark), the configuration keys are as follows:

* `bq.project` (required): Google BigQuery billing project id
* `bq.location` (required): Geographic location where newly created datasets should reside. "EU" or "US".
* `bq.service_account_key_file` (optional): Google Cloud service account key file to use for authentication with Google Cloud services. The use of service accounts is highly recommended. Specifically, the service account will be used to interact with BigQuery and Google Cloud Storage (GCS). If not specified, application default credentials will be used.
* `bq.staging_dataset.name` (optional): Prefix of BigQuery staging dataset. A staging dataset is used to temporarily store the results of SQL queries. Defaults to "spark_staging".
* `bq.staging_dataset.lifetime` (optional): Default staging dataset table lifetime in milliseconds. Tables are automatically deleted once the lifetime has been reached. Defaults to 86400000 ms (= 1 day).
* `bq.staging_dataset.gcs_bucket` (required): Google Cloud Storage (GCS) bucket to use for storing temporary files. Temporary files are used when importing through BigQuery load jobs and exporting through BigQuery extraction jobs (i.e. when using data extracts such as Parquet, Avro, ORC, ...). The service account specified in `bq.service_account_key_file` needs to be given appropriate rights.
* `bq.job.priority` (optional): BigQuery job priority when executing SQL queries. Options include "interactive" and "batch". Defaults to "interactive", i.e. the query is executed as soon as possible.
* `bq.job.timeout` (optional): Timeout in milliseconds after which a file import/export job should be considered as failed. Defaults to 3600000 ms (= 1 h).


See the following resources for more information:
* [BigQuery pricing](https://cloud.google.com/bigquery/pricing)
* [BigQuery dataset locations](https://cloud.google.com/bigquery/docs/dataset-locations)
* [General authentication](https://cloud.google.com/docs/authentication/)
* [BigQuery authentication](https://cloud.google.com/bigquery/docs/authentication/)
* [Cloud Storage authentication](https://cloud.google.com/storage/docs/authentication/)


## Schema Conversion

### Using Direct Mode

In **direct** (streaming) mode, spark-bigquery performs the following data type conversions between supported Spark data types and BigQuery data types:

**Importing to Spark / Exporting from BigQuery**

| BigQuery Source Data Type | Spark Target Data Type |
| ------------------------- | ---------------------- |
| BOOL                      | BooleanType            |
| INT64                     | LongType               |
| FLOAT64                   | DoubleType             |
| NUMERIC                   | DecimalType(38, 9)     |
| STRING                    | StringType             |
| BYTES                     | BinaryType             |
| STRUCT                    | StructType             |
| TIMESTAMP                 | TimestampType          |
| DATE                      | DateType               |
| TIME                      | StringType             |
| DATETIME                  | StringType             |

BigQuery repeated fields are mapped to the corresponding Spark ArrayType. Also, BigQuery's nullable mode is used to determine the appropriate nullable property of the target Spark data type.

**Exporting from Spark / Importing to BigQuery**

| Spark Source Data Type | BigQuery Target Data Type |
| ---------------------- | ------------------------- |
| BooleanType            | BOOL                      |
| ByteType               | INT64                     |
| ShortType              | INT64                     |
| IntegerType            | INT64                     |
| LongType               | INT64                     |
| FloatType              | FLOAT64                   |
| DoubleType             | FLOAT64                   |
| <= DecimalType(38, 9)  | NUMERIC                   |
|  > DecimalType(38, 9)  | STRING                    |
| StringType             | STRING                    |
| BinaryType             | BYTES                     |
| StructType             | STRUCT                    |
| TimestampType          | TIMESTAMP                 |
| DateType               | DATE                      |
| MapType                | Repeated key-value STRUCT |
| ArrayType              | Repeated field            |
| Other                  | STRING                    |

For more information on supported BigQuery data types see https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types.

### Using GCS Data Extracts

When using intermediate GCS data extracts (Parquet, Avro, ORC, ...) the result depends on the data format being used. Consult the data format's specification for information on supported data types. Furthermore, see the following resources on type conversions supported by BigQuery:

* Parquet: https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-parquet
* Avro: https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-avro
* ORC: https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-orc
* JSON: https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-json
* CSV: https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-csv

## Authentication

When running outside of the Google Cloud it is necessary to specify a service account JSON key file. Information on how to generate service account credentials can be found at https://cloud.google.com/storage/docs/authentication#service_accounts. The service account key file can either be passed directly via `BigQueryConfig` or it can be passed through an environment variable: `export GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/service_account_keyfile.json` (see https://cloud.google.com/docs/authentication/getting-started for more information). When running on Google Cloud, e.g. Google Cloud Dataproc, [application default credentials](https://developers.google.com/identity/protocols/application-default-credentials) may be used in which case it is not necessary to specify a service account key file.


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
