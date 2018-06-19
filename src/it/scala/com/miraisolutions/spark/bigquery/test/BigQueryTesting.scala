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

package com.miraisolutions.spark.bigquery.test

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, RDDComparisons}
import org.apache.spark.sql.DataFrame
import org.scalatest.TestSuite

trait BigQueryTesting extends BigQueryConfiguration with DataFrameSuiteBase with RDDComparisons { this: TestSuite =>

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
}
