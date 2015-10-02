package org.influxdb

import org.scalatest.{ BeforeAndAfter, FunSuite }

class ClientTest  extends FunSuite with BeforeAndAfter {
	private var client: Client = null

	final val DB_NAME               = "local-test"
	final val DB_USER               = "user"
	final val DB_PASSWORD           = "password"
	final val CLUSTER_ADMIN_USER    = "admin"
	final val CLUSTER_ADMIN_PASS    = "pass"
	final val CLUSTER_ADMIN_NEWPASS = "new pass"


	before {
		client = new Client
	}

	after {
		client.close()
	}

	test("ping") {  
		assert(client.ping.isEmpty)
	}

	test("create|get|delete database") {
		assert(client.createDatabase(DB_NAME).isEmpty)

		val (dbs, err) = client.getDatabaseList
		assert(err.isEmpty)
		assert(Nil != dbs.filter { db => db.name == DB_NAME })
		assert(client.deleteDatabase(DB_NAME).isEmpty)
	}

	test("create|get|delete database user") {
    assert(client.createUser(DB_USER, DB_PASSWORD, isAdmin = false).isEmpty)

    val (users, err) = client.getUserList
    assert(err.isEmpty)
    assert(Nil != users.filter { user => user.name == DB_USER })
    assert(client.deleteUser(DB_USER).isEmpty)
	}

	test("write|query series") {
		assert(client.createDatabase(DB_NAME).isEmpty)

    assert(client.writeSeries(Array(
        Series(name = "events", keys = Map("state" -> "ny", "email" -> "paul@influxdb.org"), fields = Map("type" -> "follow")),
        Series(name = "events", keys = Map("state" -> "ny", "email" -> "todd@influxdb.org"), fields = Map("type" -> "open")),
        Series(name = "errors", keys = Map("class" -> "DivideByZero", "file" -> "example.py", "user" -> "someguy@influxdb.org"), fields = Map("severity" -> "fatal"))
      ), DB_NAME).isEmpty)

		val (response, err) = client.queryDatabase("SELECT email FROM events WHERE type = 'follow'", DB_NAME)
		assert(err.isEmpty)

    assert(response.head.values.head.head.asInstanceOf[String] == "paul@influxdb.org")

    assert(response.head.toSeriesMap.head("email") == "paul@influxdb.org")

		assert(client.deleteDatabase(DB_NAME).isEmpty)
	}

}
