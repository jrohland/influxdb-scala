package org.influxdb.response

case class NewSeries(columns: List[String], name: String, values: List[List[String]])
case class NewResults(series: List[NewSeries])
case class NewResponse(results: List[NewResults])
