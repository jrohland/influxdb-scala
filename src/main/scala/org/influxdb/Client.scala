package org.influxdb

import org.influxdb.enums.{TimePrecision, Privilege}
import org.json4s.jackson.Serialization
import org.json4s.NoTypeHints
import com.ning.http.client.{Response, AsyncHttpClient}
import java.util.concurrent.{Future, TimeUnit}
import org.json4s.jackson.Serialization._
import java.net.URLEncoder


class Client(host: String = "localhost:8086",
             var username: String = "root",
             var password: String = "root",
             var database: String = "",
             schema: String = "http") {

  implicit val formats = Serialization.formats(NoTypeHints)
  private val httpClient = new AsyncHttpClient()

  var (timeout, unit) = (60, TimeUnit.SECONDS)

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
              new response.Database(v)
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

  def query(query: String, epoch: Option[TimePrecision] = None): (List[response.Series], error.Error) = {
    try {
      val params: Map[String, String] = Map("db" -> database, "q" -> query) ++
        (if (epoch.isDefined) Map("epoch" -> epoch.get.toString()) else Map())
      val url = getUrl(s"/query") + "&" + createQueryString(params)
      val r = getResponse(httpClient.prepareGet(url).execute())
      val series = read[response.Response](r.getResponseBody).results.head.series
      (series, responseToError(r))
    } catch {
      case ex: Exception => (null, Some(ex.getMessage))
    }
  }

  def writeSeries(series: Array[Series]): error.Error = writeSeriesCommon(series, None)

  def writeSeriesWithTimePrecision(series: Array[Series], timePrecision: String): error.Error = {
    writeSeriesCommon(series, Some(Map[String, String]("time_precision" -> timePrecision)))
  }

  private def writeSeriesCommon(series: Array[Series], options: Option[Map[String, String]]): error.Error = {
    try {
      val url = getUrl(s"/db/$database/series") + (if (options.isDefined) options.get.map { o => val (k, v) = o; s"$k=$v" }.mkString("&", "&", "") else "")
      val data = write(series)

      val fr = httpClient.preparePost(url).addHeader("Content-Type", "application/json").setBody(data).execute()
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

  private def getResponse(fr: Future[Response]): Response = fr.get(timeout, unit)
  private def getUrlWithUserAndPass(path: String, username: String, password: String): String = s"$schema://$host$path?u=$username&p=$password"
  private def getUrl(path: String) = getUrlWithUserAndPass(path, username, password)
}
