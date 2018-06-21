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

import com.miraisolutions.spark.bigquery.test._
import com.miraisolutions.spark.bigquery.test.data.{DataFrameGenerator, TestData}
import com.miraisolutions.spark.bigquery.utils.FormatConverter
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.scalatest.FunSuite
import org.scalatest.prop.{Checkers, GeneratorDrivenPropertyChecks}
import FormatConverter._

/**
  * Test suite which tests reading and writing single Spark fields/columns to and from BigQuery.
  *
  * Data frames are written to BigQuery via Parquet export to ensure data is immediately available for querying and
  * doesn't end up in BigQuery's streaming buffer as it would when using "direct" mode.
  *
  * @see [[https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-parquet]]
  * @see [[https://cloud.google.com/bigquery/streaming-data-into-bigquery]]
  */
class ReadWriteSingleFieldSpec extends FunSuite with BigQueryTesting with Checkers with GeneratorDrivenPropertyChecks {

  override implicit val generatorDrivenConfig =
    PropertyCheckConfiguration(minSuccessful = 1, minSize = 10, sizeRange = 10)

  private val testTable = "test"
  private val testFields = TestData.atomicFields ++ TestData.arrayFields ++ TestData.mapFields

  testFields foreach { field =>

    test(s"Columns of type ${field.dataType} (nullable: ${field.nullable}) " +
      s"can be written to and read from BigQuery") {

      val schema = StructType(List(field))
      implicit val arbitraryDataFrame = DataFrameGenerator.generate(sqlContext, schema)

      forAll { out: DataFrame =>

        out.write
          .mode(SaveMode.Overwrite)
          .bigqueryTest(testTable, exportType = "parquet")
          .save()

        val in = spark.read
          .bigqueryTest(testTable)
          .load()
          .persist()

        val inTransformed = transform(in, List(parquetListToArray, parquetMapToMap))

        assertDataFrameEquals(out.aligned, inTransformed.aligned)
      }
    }
  }

}
