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

package com.miraisolutions.spark.bigquery.utils

import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate, ZoneId}
import java.time.format.DateTimeFormatter

private[bigquery] object DateTime {

  private val UTC = ZoneId.of("UTC")
  private val DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE.withZone(UTC)

  /**
    * Converts a [[java.sql.Date]] to a string in the format 'yyyy-MM-dd'.
    * @param date Date
    * @return Date string of the form 'yyyy-MM-dd'
    */
  def formatDate(date: Date): String = {
    val instant = Instant.ofEpochMilli(date.getTime)
    DATE_FORMATTER.format(instant)
  }

  /**
    * Parses a string of the form 'yyyy-MM-dd' to a [[java.sql.Date]].
    * @param s String of the form 'yyyy-MM-dd'
    * @return Date
    */
  def parseDate(s: String): Date = {
    val localDate = LocalDate.parse(s, DATE_FORMATTER)
    new Date(localDate.atStartOfDay.atZone(UTC).toInstant.toEpochMilli)
  }

  /**
    * Creates a [[java.sql.Timestamp]] from microseconds since the epoch as returned by the BigQuery API.
    * @param m Microseconds since the epoch
    * @return Timestamp
    */
  def epochMicrosToTimestamp(m: Long): Timestamp = {
    val ts = new Timestamp(0)
    val nanos = (m % 1000000L).toInt * 1000

    if(nanos < 0) {
      ts.setTime((m / 1000000L - 1L) * 1000L)
      ts.setNanos(nanos + 1000000000)
    } else {
      ts.setTime(m / 1000000L * 1000L)
      ts.setNanos(nanos)
    }

    ts
  }

  /**
    * Converts a [[java.sql.Timestamp]] to the number of (fractional) seconds since the epoch.
    * @param ts Timestamp
    * @return Seconds since the epoch
    */
  def timestampToEpochSeconds(ts: Timestamp): Double = {
    ts.getTime / 1000L + ts.getNanos.toDouble / 1e9
  }
}
