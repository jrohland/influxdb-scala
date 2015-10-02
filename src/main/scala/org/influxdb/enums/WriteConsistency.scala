package org.influxdb.enums

object WriteConsistency extends Enumeration {
  type WriteConsistency = Value
  val ALL = Value("all")
  val ANY = Value("any")
  val ONE = Value("one")
  val QUORUM = Value("quorum")
}
