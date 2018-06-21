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

package com.miraisolutions.spark.bigquery.test.data

import java.math.BigInteger
import java.sql.{Date, Timestamp}
import java.time._

import com.holdenkarau.spark.testing.RDDGenerator
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types._
import org.scalacheck.{Arbitrary, Gen}

/**
  * Generator of arbitrary Spark data frames used in property-based testing.
  */
private[bigquery] object DataFrameGenerator {

  // Min and max BigQuery timestamp and date values
  // See https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types
  private val MIN_INSTANT = Instant.parse("0001-01-01T00:00:00.000000Z")
  private val MAX_INSTANT = Instant.parse("9999-12-31T23:59:59.999999Z")
  // Number of milliseconds in one day
  private val MILLIS_PER_DAY = 86400000L

  /**
    * Generates an arbitrary Spark data frame with the specified schema and minimum number of partitions.
    * @param sqlContext Spark SQL context
    * @param schema Schema of data frame to generate
    * @param minPartitions Minimum number of partitions
    * @return Arbitrary Spark data frame
    */
  def generate(sqlContext: SQLContext, schema: StructType, minPartitions: Int = 1): Arbitrary[DataFrame] = {
    val genRow = getRowGenerator(schema)
    val genDataFrame = RDDGenerator.genRDD[Row](sqlContext.sparkContext, minPartitions)(genRow)
    Arbitrary(genDataFrame.map(sqlContext.createDataFrame(_, schema)))
  }

  /**
    * Creates a generator of a row in a Spark data frame.
    * @param schema Schema of row to generate
    * @return Generator for a row
    */
  private def getRowGenerator(schema: StructType): Gen[Row] = {
    import scala.collection.JavaConverters._
    val fieldGenerators = schema.fields.map(field => getGeneratorForType(field.dataType))
    val rowGen = Gen.sequence(fieldGenerators)
    rowGen.map(values => Row.fromSeq(values.asScala))
  }

  /**
    * Creates a generator for a target data type.
    * @param dataType Data type
    * @return Generator of values of the specified data type
    */
  private def getGeneratorForType(dataType: DataType): Gen[Any] = {
    import Arbitrary._

    dataType match {
      case ByteType =>
        arbitrary[Byte]

      case ShortType =>
        arbitrary[Short]

      case IntegerType =>
        arbitrary[Int]

      case LongType =>
        arbitrary[Long]

      case FloatType =>
        arbitrary[Float]

      case DoubleType =>
        arbitrary[Double]

      case dt: DecimalType =>
        for {
          digits <- Gen.listOfN(dt.precision, Gen.numChar)
          sign <- Gen.oneOf("", "-")
          unscaledValue = new BigInteger(sign + digits.mkString)
        } yield new java.math.BigDecimal(unscaledValue, dt.scale)

      case StringType =>
        arbitrary[String]

      case BinaryType =>
        Gen.listOf(arbitrary[Byte]).map(_.toArray)

      case BooleanType =>
        arbitrary[Boolean]

      case TimestampType =>
        // See https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types
        // BigQuery allowed timestamp range: [0001-01-1 00:00:00.000000, 9999-12-31 23:59:59.999999]
        Gen.chooseNum[Long](MIN_INSTANT.toEpochMilli, MAX_INSTANT.toEpochMilli).map(new Timestamp(_))

      case DateType =>
        // See https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types
        // BigQuery allowed date range: [0001-01-1, 9999-12-31]
        Gen.chooseNum[Long](MIN_INSTANT.toEpochMilli, MAX_INSTANT.toEpochMilli) map { millis =>
          // We need to round the milliseconds to full days as otherwise the time components will be set to the
          // time components in the default time zone; see javadoc for java.sql.Date for more details
          new Date(millis / MILLIS_PER_DAY * MILLIS_PER_DAY)
        }

      case arr: ArrayType =>
        val elementGenerator = getGeneratorForType(arr.elementType)
        Gen.listOf(elementGenerator)

      case map: MapType =>
        val keyGenerator = getGeneratorForType(map.keyType)
        val valueGenerator = getGeneratorForType(map.valueType)
        val keyValueGenerator: Gen[(Any, Any)] = for {
          key <- keyGenerator
          value <- valueGenerator
        } yield (key, value)

        Gen.mapOf(keyValueGenerator)

      case row: StructType =>
        getRowGenerator(row)

      case _ =>
        throw new UnsupportedOperationException(s"Data type '$dataType' is not supported")
    }
  }
}
