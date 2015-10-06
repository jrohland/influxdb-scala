#influxdb-scala

## Scala client for InfluxDB 0.9.x

### Dependencies
>org.json4s:json4s-jackson:3.2.11

>com.ning:async-http-clien:1.0.0

>com.google.guava:guava:18.0

### Usage
```
import java.util.Date
import java.util.concurrent.TimeUnit
import org.influxdb.{Series, Client}

val client = new Client(host = "localhost:8086", username = "root", password = "root")

client.createDatabase("testdb") match {
  case Some(error) => throw new Exception(error)
  case _ => println("Database created")
}

val time = new Date().getTime / 1000

val series = Array(
  new Series(name = "testseries", keys = Map("name" -> "one", "location" -> "ny"), fields = Map("value" -> 1), time = time, timePrecision = TimeUnit.SECONDS),
  new Series(name = "testseries", keys = Map("name" -> "one", "location" -> "ny"), fields = Map("value" -> 2), time = time + 1, timePrecision = TimeUnit.SECONDS),
  new Series(name = "testseries", keys = Map("name" -> "two", "location" -> "ny"), fields = Map("value" -> 2), time = time, timePrecision = TimeUnit.SECONDS)
)

client.writeSeries(series = series, database = "testdb", timePrecision = TimeUnit.SECONDS) match {
  case Some(error) => throw new Exception(error)
  case _ => println("Series written")
}

val results = client.queryDatabase(queryStr = s"SELECT SUM(value) FROM testseries WHERE time >= ${time}s GROUP BY time(1h),name,location", database = "testdb")

results._2 match {
  case Some(error) => throw new Exception(error)
  case _ => 
    println(s"${results._1.head.tags.get.keySet.mkString(",")},${results._1.head.columns.mkString(",")}")
    println("---------------------")
    results._1.foreach(series => {
      println(s"${series.tags.get.values.mkString(",")},${series.values.head.mkString(",")}")
    })
}

client.deleteDatabase("testdb") match {
  case Some(error) => throw new Exception(error)
  case _ => println("Database deleted")
}

```

Output
```
Database created
Series written
location,name,time,sum
---------------------
ny,one,2015-10-06T02:22:29Z,3
ny,two,2015-10-06T02:22:29Z,2
Database deleted
```