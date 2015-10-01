package org.influxdb

case class Series(name: String, columns: Array[String], points: Array[Array[Any]])
