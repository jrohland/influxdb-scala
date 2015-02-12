package org.influxdb.response

import org.influxdb.{SeriesMap, Series}

import scala.util.parsing.json.JSON

case class Response(json: String) {

  def toSeries: Array[Series] = {
    val all = JSON.parseFull(json).get.asInstanceOf[List[Any]]
    val series = new Array[Series](all.length)

    var i = 0
    all.foreach { ai =>
      val m = ai.asInstanceOf[Map[String, Any]]
      val name = m.get("name").get.asInstanceOf[String]
      val columns = m.get("columns").get.asInstanceOf[List[String]].toArray
      val points = m.get("points").get.asInstanceOf[List[List[Any]]].map(li => li.toArray).toArray

      series(i) = Series(name, columns, points)
      i += 1
    }
    series
  }

  def toSeriesMap: Array[SeriesMap] = {
    val all = JSON.parseFull(json).get.asInstanceOf[List[Any]]
    val series = new Array[SeriesMap](all.length)

    var i = 0
    all.foreach { ai =>
      val m = ai.asInstanceOf[Map[String, Any]]
      val name = m.get("name").get.asInstanceOf[String]
      val columns = m.get("columns").get.asInstanceOf[List[String]]

      var ii = 0
      val mm = scala.collection.mutable.Map[String, Array[Any]]()
      val cc = new Array[String](columns.size)
      columns.foreach { cl => cc(ii) = cl; mm(cl) = Array[Any](); ii += 1 }

      m.get("points").get.asInstanceOf[List[List[Any]]].foreach { pt =>
        ii = 0
        pt.foreach { v => mm += cc(ii) -> (mm(cc(ii)) :+ v); ii += 1; }
      }
      series(i) = SeriesMap(name, mm.toMap)
      i += 1
    }
    series
  }
}
