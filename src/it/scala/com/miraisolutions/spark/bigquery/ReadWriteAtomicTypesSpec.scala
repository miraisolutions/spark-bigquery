package com.miraisolutions.spark.bigquery

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, RDDComparisons}
import com.miraisolutions.spark.bigquery.data.{DataFrameGenerator, TestData}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.scalatest.FunSuite
import org.scalatest.prop.{Checkers, GeneratorDrivenPropertyChecks}

/**
  * Test suite which tests reading and writing Spark atomic types to and from BigQuery.
  *
  * Run in sbt via:
  *
  * it:testOnly com.miraisolutions.spark.bigquery.* --
  * -Dbq.project=<project>
  * -Dbq.staging_dataset.location=<location>
  * -Dbq.staging_dataset.gcs_bucket=<gcs_bucket>
  * -DsvcAccountKeyFile=<path_to_keyfile>
  */
class ReadWriteAtomicTypesSpec extends FunSuite with DataFrameSuiteBase with RDDComparisons with Checkers
  with GeneratorDrivenPropertyChecks with BigQueryConfiguration {

  // See https://github.com/holdenk/spark-testing-base/issues/148
  // See https://issues.apache.org/jira/browse/SPARK-22918
  System.setSecurityManager(null)

  override def assertDataFrameEquals(expected: DataFrame, result: DataFrame): Unit = {
    assert("Schemas don't match", expected.schema, result.schema)
    assert("Number of rows don't match", expected.count(), result.count())

    val mismatch = compareRDD(expected.rdd, result.rdd)
    if(mismatch.isDefined) {
      println("#### Expected ####")
      expected.show(10, 100, true)
      println("#### Result ####")
      result.show(10, 100, true)
    }

    assertTrue(mismatch.isEmpty)
  }

  TestData.atomicFields foreach { field =>

    test(s"Columns of type ${field.dataType} (nullable: ${field.nullable}) can be written to and read from BigQuery") {

      implicit val generatorDrivenConfig = PropertyCheckConfiguration(minSuccessful = 2, minSize = 10, sizeRange = 20)

      val schema = StructType(List(field))
      implicit val arbitraryDataFrame = DataFrameGenerator.generate(sqlContext, schema)

      forAll { df: DataFrame =>

        df.write
          .mode(SaveMode.Overwrite)
          .withBigQueryTestOptions(table = "test")
          .option("type", "parquet")
          .save()

        val x = spark.read
          .withBigQueryTestOptions(table = "test")
          .option("type", "direct")
          .load()
          .persist()

        assertDataFrameEquals(df.aligned, x)
      }
    }
  }
}
