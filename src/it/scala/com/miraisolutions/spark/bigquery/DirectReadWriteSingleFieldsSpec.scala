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

import com.miraisolutions.spark.bigquery.client.BigQueryClient
import com.miraisolutions.spark.bigquery.test._
import com.miraisolutions.spark.bigquery.test.data.{DataFrameGenerator, TestData}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.scalatest.FunSuite
import org.scalatest.prop.{Checkers, GeneratorDrivenPropertyChecks}

/**
  * Test suite which tests reading and writing single Spark fields/columns to and from BigQuery.
  *
  * Data frames are written to BigQuery via "direct" export. Because attempts to query the new fields might require
  * a waiting time of up to 90 minutes (https://cloud.google.com/bigquery/streaming-data-into-bigquery), this test suite
  * only verifies the correctness of the generated BigQuery schema.
  */
class DirectReadWriteSingleFieldsSpec extends FunSuite with BigQueryTesting with Checkers
  with GeneratorDrivenPropertyChecks {

  override implicit val generatorDrivenConfig =
    PropertyCheckConfiguration(minSuccessful = 2, minSize = 10, sizeRange = 20)

  private val bigQueryClient = new BigQueryClient(config)
  private val testTable = "direct_test"
  private val testFields = TestData.atomicFields ++ TestData.arrayFields

  testFields foreach { field =>

    test(s"Columns of type ${field.dataType} (nullable: ${field.nullable}) " +
      "can be written to and read from BigQuery using \"direct\" imports (streaming)") {

      val schema = StructType(List(field))
      implicit val arbitraryDataFrame = DataFrameGenerator.generate(sqlContext, schema)
      val tableName = testTable + "_" + System.currentTimeMillis().toString

      forAll { df: DataFrame =>
        df.write
          .mode(SaveMode.Overwrite)
          .bigqueryTest(tableName)
          .save()

        val in = spark.read
          .bigqueryTest(tableName)
          .load()
          .persist()

        val tableReference = getTestDatasetTableReference(tableName)

        assert(df.aligned.schema, in.aligned.schema)
        val deleted = bigQueryClient.getTable(tableReference, 1).table.delete()
        assert(deleted, true)
      }
    }
  }
}
