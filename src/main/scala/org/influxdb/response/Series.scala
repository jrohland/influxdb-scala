package org.influxdb.response

case class Series(columns: List[String], name: Option[String], values: List[List[Any]])