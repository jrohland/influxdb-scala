package org.influxdb.enums

object Privilege extends Enumeration {
  type Privilege = Value
  val READ, WRITE, ALL = Value
}
