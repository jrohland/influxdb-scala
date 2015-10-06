package org.influxdb

import java.net.URLEncoder
import java.util.concurrent.{Future, TimeUnit}

import com.ning.http.client.{AsyncHttpClient, Response}
import org.influxdb.enums.Privilege._
import org.influxdb.enums.WriteConsistency
import org.influxdb.enums.WriteConsistency._
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._


class Client(host: String = "localhost:8086",
             var username: String = "root",
             var password: String = "root",
             schema: String = "http",
             timeout: Int = 60,
             timeoutUnit: TimeUnit = TimeUnit.SECONDS) {

  implicit val formats = Serialization.formats(NoTypeHints)
  private val httpClient = new AsyncHttpClient()

  def close() {
    httpClient.close()
  }

  def ping: error.Error = {
    try {
      val url = getUrl("/ping")
      responseToError(getResponse(httpClient.prepareGet(url).execute()))
    } catch {
      case ex: Exception => Some(ex.getMessage)
    }
  }

  def createDatabase(name: String): error.Error = {
    query(s"CREATE DATABASE $name")._2
  }

  def deleteDatabase(name: String): error.Error = {
    query(s"DROP DATABASE $name")._2
  }

  def getDatabaseList: (List[response.Database], error.Error) = {
    val r = query("SHOW DATABASES")
    r._2 match {
      case None =>
        val databases = r._1.flatMap(series => {
          series.values.flatMap(value => {
            value.map(v => {
              new response.Database(v.asInstanceOf[String])
            })
          })
        })
        (databases, None)
      case Some(err) => (Nil, Some(err))
    }
  }

  def createUser(username: String, password: String, isAdmin: Boolean): error.Error = {
    val q = s"CREATE USER $username WITH PASSWORD '$password'" +
      (if (isAdmin) " WITH ALL PRIVILEGES" else "")
    val r = query(q)
    r._2
  }

  def updateUserPassword(username: String, password: String): error.Error = {
    val q = s"SET PASSWORD FOR $username = '$password'"
    val r = query(q)
    r._2
  }

  def grantUserPrivilege(username: String, database: String, privilege: Privilege): error.Error = {
    val q = s"GRANT $privilege ON $database TO $username"
    val r = query(q)
    r._2
  }

  def grantUserAllPrivilege(username: String): error.Error = {
    val q = s"GRANT ALL PRIVILEGES TO $username"
    val r = query(q)
    r._2
  }

  def revokeUserPrivilege(username: String, database: String, privilege: Privilege): error.Error = {
    val q = s"REVOKE $privilege ON $database TO $username"
    val r = query(q)
    r._2
  }

  def revokeUserAllPrivilege(username: String): error.Error = {
    val q = s"REVOKE ALL PRIVILEGES TO $username"
    val r = query(q)
    r._2
  }

  def deleteUser(name: String): error.Error = {
    query(s"DROP USER $name")._2
  }

  def getUserList: (List[response.User], error.Error) = {
    val r = query("SHOW USERS")
    r._2 match {
      case None =>
        val users = r._1.flatMap(series => {
          series.values.map(value => {
            new response.User(value.head.asInstanceOf[String], value(1).asInstanceOf[Boolean])
          })
        })
        (users, None)
      case Some(err) => (Nil, Some(err))
    }
  }

  def query(queryStr: String, database: Option[String] = None, timePrecision: Option[TimeUnit] = None):
  (List[response.Series], error.Error) = {
    try {
      val params: Map[String, String] = Map("q" -> queryStr) ++
        (if (database.isDefined) {
          Map("db" -> database.get)
        } else {
          Map()
        }) ++
        (if (timePrecision.isDefined) {
          val epoch = timePrecision.get match {
            case TimeUnit.HOURS => "h"
            case TimeUnit.MINUTES => "m"
            case TimeUnit.SECONDS => "s"
            case TimeUnit.MILLISECONDS => "ms"
            case TimeUnit.MICROSECONDS => "u"
            case _ => "ns"
          }
          Map("epoch" -> epoch)
        } else {
          Map()
        })
      val url = getUrl(s"/query", Option(params))
      val r = getResponse(httpClient.prepareGet(url).execute())
      val results = read[response.Response](r.getResponseBody).results.head
      if (results.error.isDefined) {
        (null, Some(results.error.get))
      } else if (results.series.isDefined) {
        (results.series.get, responseToError(r))
      } else {
        (null, responseToError(r))
      }
    } catch {
      case ex: Exception => (null, Some(ex.getMessage))
    }
  }

  def queryDatabase(queryStr: String, database: String, timePrecision: Option[TimeUnit] = None):
  (List[response.Series], error.Error) = query(queryStr, Option(database), timePrecision)

  def writeSeries(series: Array[Series],
                  database: String,
                  timePrecision: TimeUnit = TimeUnit.NANOSECONDS,
                  writeConsistency: WriteConsistency = WriteConsistency.ALL,
                  retentionPolicy: Option[String] = None): error.Error = {

    val precisionStr = timePrecision match {
      case TimeUnit.HOURS => "h"
      case TimeUnit.MINUTES => "m"
      case TimeUnit.SECONDS => "s"
      case TimeUnit.MILLISECONDS => "ms"
      case TimeUnit.MICROSECONDS => "u"
      case TimeUnit.NANOSECONDS => "n"
      case _ => return Some("Invalid time precision")
    }

    try {
      val params: Map[String, String] = Map(
        "db" -> database,
        "precision" -> precisionStr,
        "consistency" -> writeConsistency.toString
      ) ++ (if (retentionPolicy.isDefined) Map("rp" -> retentionPolicy.get) else Map())

      val url = getUrl(s"/write", Option(params))
      val data = series.map(series => {
        val time = timePrecision.convert(series.time, series.timePrecision)
        s"${series.tagsStr} ${series.valuesStr} $time"
      }).mkString("\n")

      val fr = httpClient.preparePost(url).setBody(data).execute()
      responseToError(getResponse(fr))
    } catch {
      case ex: Exception => Some(ex.getMessage)
    }
  }

  private def responseToError(r: Response): error.Error = {
    if (r.getStatusCode >= 200 && r.getStatusCode < 300) {
      return None
    }
    Some(s"Server returned (${r.getStatusText}): ${r.getResponseBody}")
  }

  private def createQueryString(params: Map[String, String]): String = {
    params.map(v => {
      URLEncoder.encode(v._1, "UTF8") + "=" + URLEncoder.encode(v._2, "UTF8")
    }).mkString("&")
  }

  private def getResponse(fr: Future[Response]): Response = fr.get(timeout, timeoutUnit)

  private def getUrl(path: String, params: Option[Map[String, String]] = None) = {
    val paramsWithUser = Map(
      "u" -> username,
      "p" -> password
    ) ++ (
      if (params.isDefined) {
        params.get
      } else {
        Map()
      }
    )

    s"$schema://$host$path?${createQueryString(paramsWithUser)}"
  }
}
