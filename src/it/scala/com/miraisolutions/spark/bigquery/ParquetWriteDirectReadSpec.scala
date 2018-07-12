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
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.scalactic.anyvals.PosZInt
import org.scalatest.FunSuite
import org.scalatest.prop.{Checkers, GeneratorDrivenPropertyChecks}

/**
  * Test suite which tests reading and writing Spark fields/columns to and from BigQuery.
  *
  * Data frames are written to BigQuery via Parquet export to ensure data is immediately available for querying and
  * doesn't end up in BigQuery's streaming buffer as it would when using "direct" mode.
  *
  * @see [[https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-parquet]]
  * @see [[https://cloud.google.com/bigquery/streaming-data-into-bigquery]]
  * @see [[https://cloud.google.com/blog/big-data/2017/06/life-of-a-bigquery-streaming-insert]]
  */
class ParquetWriteDirectReadSpec extends FunSuite with BigQueryTesting {

  private val testTable = "test"

  private class RandomDataFrame(schema: StructType, size: PosZInt)
    extends Checkers with GeneratorDrivenPropertyChecks {

    override implicit val generatorDrivenConfig =
      PropertyCheckConfiguration(minSuccessful = 1, minSize = size, sizeRange = size)

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

      assertDataFrameEquals(out.aligned, in.aligned)
    }

  }

  (TestData.atomicFields ++ TestData.arrayFields ++ TestData.mapFields) foreach { field =>

    test(s"Column of type ${field.dataType} (nullable: ${field.nullable}) " +
      s"can be written to and read from BigQuery") {
      new RandomDataFrame(StructType(List(field)), 10)
    }

  }

  test("Nested struct columns can be written to and read from BigQuery") {
    new RandomDataFrame(StructType(List(TestData.customStructField)), 2)
  }

  test("Data frames with mixed data types can be written to and read from BigQuery") {
    new RandomDataFrame(StructType(TestData.atomicFields ++ TestData.arrayFields.take(2) ++
      TestData.mapFields.take(2)), 2)
  }

}
