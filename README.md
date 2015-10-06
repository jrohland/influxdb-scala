#influxdb-scala

## Scala client for InfluxDB 0.9.x

### Dependencies
>org.json4s:json4s-jackson:3.2.11

>com.ning:async-http-clien:1.0.0

>com.google.guava:guava:18.0

### Usage

#### Connection
```
import org.influxdb.Client
val client = new Client(host = "localhost:8086", username = "root", password = "root")
```

#### Database Operations
##### Create Database
```
client.Database.create("testdb") match {
  case Some(error) => throw new Exception(error)
  case _ => println("Database created")
}
```

##### List Databases
```
val results = client.Database.list
results._2 match {
  case Some(error) => throw new Exception(error)
  case _ => results._1.foreach(println(_))
}
```

##### Delete Database
```
client.Database.delete("testdb") match {
  case Some(error) => throw new Exception(error)
  case _ => println("Database deleted")
}
```

#### Writing Data
```
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
```

#### Querying Data
```
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
```

#### User Operations
##### Create User
```
client.User.create(username = "testuser", password = "password", isAdmin = false) match {
  case Some(error) => throw new Exception(error)
  case _ => println("User created")
}
```

##### List Users
```
val results = client.User.list
results._2 match {
  case Some(error) => throw new Exception(error)
  case _ => results._1.foreach(user => println(user.name))
}
```

##### Update User Password
```
client.User.updatePassword(username = "testuser", password = "newpassword") match {
  case Some(error) => throw new Exception(error)
  case _ => println("Password updated")
}
```

##### Grant Specific Privilege on Database
```
client.User.grantPrivilege(username = "testuser", database = "testdb", privilege = Privilege.ALL) match {
  case Some(error) => throw new Exception(error)
  case _ => println("Privilege granted")
}
```

##### Grant All Privilege
```
client.User.grantAllPrivilege(username = "testuser") match {
  case Some(error) => throw new Exception(error)
  case _ => println("All privileges granted")
}
```

##### Revoke Specific Privilege on Database
```
client.User.revokePrivilege(username = "testuser", database = "testdb", privilege = Privilege.ALL) match {
  case Some(error) => throw new Exception(error)
  case _ => println("Privilege revoked")
}
```

##### Revoke All Privilege
```
client.User.revokeAllPrivilege(username = "testuser") match {
  case Some(error) => throw new Exception(error)
  case _ => println("All privileges revoked")
}
```

##### Delete User
```
client.User.delete("testuser") match {
  case Some(error) => throw new Exception(error)
  case _ => println("User deleted")
}
```
