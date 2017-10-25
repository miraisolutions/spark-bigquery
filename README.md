## Sparklyr-BigQuery How-To

In order to use the Spark BigQuery `DefaultSource` you need to build a fat/uber jar in sbt via `sbt assembly`. This fat jar can then be used with `sparklyr` as follows:

```
library(sparklyr)

# GCP service account JSON key file
# see https://cloud.google.com/compute/docs/access/service-accounts
gcpJsonKeyfile <- "/home/erlik/aviato/gcp/credentials/mirai-sbb-da847ce33b19.json"

# https://developers.google.com/identity/protocols/application-default-credentials
# Sys.setenv("GOOGLE_APPLICATION_CREDENTIALS" = gcpJsonKeyfile)

config <- spark_config()
config[["sparklyr.shell.driver-class-path"]] <- "/home/erlik/aviato/sbb-history-assembly-0.2-SNAPSHOT.jar"
# config[["spark.executorEnv.GOOGLE_APPLICATION_CREDENTIALS"]] = "gcpJsonKeyfile
config[["spark.hadoop.google.cloud.auth.service.account.json.keyfile"]] = gcpJsonKeyfile

sc <- spark_connect(master = "local", config = config)
stations <-
  spark_read_source(
    sc,
    name = "stations",
    source = "com.mirai.sbbhistory.bigquery",
    options = list(
      "bq.project.id" = "mirai-sbb",
      "bq.gcs.bucket" = "sbb",
      "bq.dataset.location" = "EU",
      "tableReference" = "mirai-sbb:sbb.stations" # A string of the form [projectId]:[datasetId].[tableId]
    )
  )
```
