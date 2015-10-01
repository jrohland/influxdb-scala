package org.influxdb.response

case class Series(columns: List[String], name: String, values: List[List[String]])