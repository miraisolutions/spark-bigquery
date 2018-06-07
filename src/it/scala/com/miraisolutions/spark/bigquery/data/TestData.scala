package com.miraisolutions.spark.bigquery.data

import org.apache.spark.sql.types._

private[bigquery] object TestData {

  // Atomic types currently without DecimalType and BinaryType
  private val atomicTypes: Set[DataType] = Set(BooleanType, ByteType, ShortType, IntegerType, LongType, FloatType,
    DoubleType, StringType, TimestampType, DateType)

  private def createName(dt: DataType, nullable: Boolean): String = {
    val base = dt.simpleString.replaceAll("[^A-Za-z]+", "")
    if(nullable) {
      base + "0"
    } else {
      base
    }
  }

  val atomicFields: Set[StructField] = {
    for {
      dt <- atomicTypes
      nullable <- Set(true, false)
    } yield StructField(createName(dt, nullable), dt, nullable)
  }

  private def createName(dt: ArrayType, nullable: Boolean): String = {
    val baseType = createName(dt.elementType, dt.containsNull)
    val array = if(nullable) "array0" else "array"
    s"${array}_${baseType}_"
  }

  private val arrayTypes: Set[ArrayType] = {
    for {
      bt <- atomicTypes
      containsNull <- Set(true, false)
    } yield ArrayType(bt, containsNull)
  }

  val arrayFields: Set[StructField] = {
    for {
      at <- arrayTypes
      nullable <- Set(true, false)
    } yield StructField(createName(at, nullable), at, nullable)
  }

/*  val arrayOfArrayFields: Set[StructField] = {
    for {
      af <- arrayFields
      containsNull <- Set(true, false)
      nullable <- Set(true, false)
      at =
    } yield StructField()
  }*/
}
