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

package com.miraisolutions.spark.bigquery

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, RDDComparisons}
import com.miraisolutions.spark.bigquery.data.{DataFrameGenerator, TestData}
import org.apache.spark.sql.types.{StructType, TimestampType}
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
  * -Dbq.staging_dataset.service_account_key_file=<path_to_keyfile>
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
      expected.show(10, false)
      // expected.show(10, 100, true)
      println("#### Result ####")
      result.show(10, false)
      // result.show(10, 100, true)
    }

    assertTrue(mismatch.isEmpty)
  }

  TestData.atomicFields.filter(_.dataType == TimestampType) foreach { field =>

    test(s"Columns of type ${field.dataType} (nullable: ${field.nullable}) can be written to and read from BigQuery") {

      implicit val generatorDrivenConfig = PropertyCheckConfiguration(minSuccessful = 2, minSize = 10, sizeRange = 20)

      val schema = StructType(List(field))
      implicit val arbitraryDataFrame = DataFrameGenerator.generate(sqlContext, schema)

      val table = "test"

      forAll { df: DataFrame =>

        df.write
          .mode(SaveMode.Overwrite)
          .bigqueryTest(table, exportType = "parquet")
          .save()

        val x = spark.read
          .bigqueryTest(table)
          .load()
          .persist()

        assertDataFrameEquals(df.aligned, x)
      }
    }
  }
}
