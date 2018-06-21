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

import org.apache.spark.sql.types._

private[bigquery] object TestData {

  private val atomicTypes: List[DataType] = List(BooleanType, ByteType, ShortType, IntegerType, LongType, FloatType,
    DoubleType, StringType, BinaryType, TimestampType, DateType, DataTypes.createDecimalType(38, 9),
    DataTypes.createDecimalType(12, 4), DataTypes.createDecimalType(33, 4),
    DataTypes.createDecimalType(7,7))

  private def createName(dt: DataType, nullable: Boolean): String = {
    val base = dt.simpleString.replaceAll("[^A-Za-z]+", "")
    if(nullable) {
      base + "0"
    } else {
      base
    }
  }

  private def createFields[T <: DataType](dataTypes: List[T], createName: (T, Boolean) => String): List[StructField] = {
    for {
      dt <- dataTypes
      nullable <- List(true, false)
    } yield StructField(createName(dt, nullable), dt, nullable)
  }

  val atomicFields: List[StructField] = createFields(atomicTypes, createName)

  private def createArrayName(dt: ArrayType, nullable: Boolean): String = {
    val elementName = createName(dt.elementType, dt.containsNull)
    val array = if(nullable) "array0" else "array"
    s"${array}_${elementName}_"
  }

  private val arrayTypes: List[ArrayType] = {
    for {
      bt <- atomicTypes
      containsNull <- List(true, false)
    } yield ArrayType(bt, containsNull)
  }

  val arrayFields: List[StructField] = createFields(arrayTypes, createArrayName)

  private def createMapName(dt: MapType, nullable: Boolean): String = {
    val keyName = createName(dt.keyType, false)
    val valueName = createName(dt.valueType, dt.valueContainsNull)
    val map = if(nullable) "map0" else "map"
    s"${map}_${keyName}_${valueName}"
  }

  private val mapTypes: List[MapType] = {
    for {
      keyType <- atomicTypes
      valueType <- atomicTypes
      valueContainsNull <- Set(true, false)
    } yield MapType(keyType, valueType, valueContainsNull)
  }

  val mapFields: List[StructField] = createFields(mapTypes, createMapName)
}
