package org.influxdb.response

case class Results(series: Option[List[Series]], error: Option[String])
