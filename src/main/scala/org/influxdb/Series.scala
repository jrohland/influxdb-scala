package org.influxdb

import java.text.NumberFormat
import java.util.Locale
import java.util.concurrent.TimeUnit

import com.google.common.escape.Escapers

case class Series(name: String,
                  tags: Map[String, String],
                  values: Map[String, Any],
                  time: Long = System.nanoTime(),
                  timePrecision: TimeUnit = TimeUnit.NANOSECONDS)  {

  private val tagEscaper = Escapers.builder().addEscape(' ', "\\ ").addEscape(',', "\\,").build()
  private val valueEscaper = Escapers.builder().addEscape('"', "\\\"").build()
  private val numberFormat = NumberFormat.getInstance(Locale.ENGLISH)
  numberFormat.setMaximumFractionDigits(340)
  numberFormat.setGroupingUsed(false)
  numberFormat.setMinimumFractionDigits(1)

  def tagsStr: String = {
    if (name == null) {
      throw new NullPointerException("Null series name")
    }

    val seriesName = s"${tagEscaper.escape(name)}"
    val tagsStr = tags.map(tag => {
      if (tag._1 != null) {
        if (tag._2 != null) {
          s"${tagEscaper.escape(tag._1)}=${tagEscaper.escape(tag._2)}"
        } else {
          throw new NullPointerException(s"Null tag value for tag: ${tag._1}")
        }
      } else {
        throw new NullPointerException("Null tag name")
      }
    }).mkString(",")
    s"$seriesName,$tagsStr"
  }

  def valuesStr: String = {
    values.map(value => {
      if (value._1 != null) {
        if (value._2 != null) {
          s"${valueEscaper.escape(value._1)}=" + (
            value._2 match {
              case s: String =>
                "\"" + valueEscaper.escape(s) + "\""
              case n: java.lang.Number =>
                s"${numberFormat.format(n)}"
              case default =>
                s"$default"
            })
        } else {
          throw new NullPointerException(s"Null field value for value: ${value._1}")
        }
      } else {
        throw new NullPointerException("Null value name")
      }
    }).mkString(",")
  }

  override def toString: String = {
    s"$tagsStr $valuesStr"
  }

}
