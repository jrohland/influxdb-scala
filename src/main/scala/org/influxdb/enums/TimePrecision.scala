package org.influxdb.enums

object TimePrecision extends Enumeration {
  type TimePrecision = Value
  val HOUR = Value("h")
  val MINUTE = Value("m")
  val SECOND = Value("s")
  val MILLISECOND = Value("ms")
  val MICROSECONDS = Value("u")
  val NANOSECONDS = Value("ns")
}
