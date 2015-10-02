package org.influxdb.enums

object TimePrecision extends Enumeration {
  type TimePrecision = Value
  val h, m, s, ms, u, ns = Value
}
