package org.influxdb

import java.util.concurrent.TimeUnit

case class Series(name: String,
                  keys: Map[String, String],
                  fields: Map[String, Any],
                  time: Long = System.nanoTime(),
                  timePrecision: TimeUnit = TimeUnit.NANOSECONDS)
