package org.influxdb

case class Series(name: String, keys: Map[String, String], fields: Map[String, Any])
