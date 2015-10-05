package org.influxdb

import java.text.NumberFormat
import java.util.Locale
import java.util.concurrent.TimeUnit

import com.google.common.escape.Escapers

case class Series(name: String,
                  keys: Map[String, String],
                  fields: Map[String, Any],
                  time: Long = System.nanoTime(),
                  timePrecision: TimeUnit = TimeUnit.NANOSECONDS)  {

  private val keyEscaper = Escapers.builder().addEscape(' ', "\\ ").addEscape(',', "\\,").build()
  private val fieldEscaper = Escapers.builder().addEscape('"', "\\\"").build()
  private val numberFormat = NumberFormat.getInstance(Locale.ENGLISH)
  numberFormat.setMaximumFractionDigits(340)
  numberFormat.setGroupingUsed(false)
  numberFormat.setMinimumFractionDigits(1)

  def keysStr: String = {
    if (name == null) {
      throw new NullPointerException("Null series name")
    }

    val seriesName = s"${keyEscaper.escape(name)}"
    val keysStr = keys.map(key => {
      if (key._1 != null) {
        if (key._2 != null) {
          s"${keyEscaper.escape(key._1)}=${keyEscaper.escape(key._2)}"
        } else {
          throw new NullPointerException(s"Null key value for key: ${key._1}")
        }
      } else {
        throw new NullPointerException("Null key name")
      }
    }).mkString(",")
    s"$seriesName,$keysStr"
  }

  def fieldsStr: String = {
    fields.map(field => {
      if (field._1 != null) {
        if (field._2 != null) {
          s"${fieldEscaper.escape(field._1)}=" + (
            field._2 match {
              case s: String =>
                "\"" + fieldEscaper.escape(s) + "\""
              case n: java.lang.Number =>
                s"${numberFormat.format(n)}"
              case default =>
                s"$default"
            })
        } else {
          throw new NullPointerException(s"Null field value for field: ${field._1}")
        }
      } else {
        throw new NullPointerException("Null field name")
      }
    }).mkString(",")
  }

  override def toString: String = {
    s"$keysStr $fieldsStr"
  }

}
