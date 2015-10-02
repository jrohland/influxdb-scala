package org.influxdb.response

case class Series(columns: List[String], name: Option[String], values: List[List[Any]]) {

  def toSeriesMap: List[Map[String, Any]] = {
    values.map(valueArray => {
      var i = -1
      valueArray.map(value => {
        i += 1
        (columns(i), value)
      }).toMap
    })
  }

}