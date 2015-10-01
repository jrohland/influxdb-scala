package org.influxdb

import org.influxdb.response.NewResponse
import org.json4s.jackson.Serialization
import org.json4s.NoTypeHints
import com.ning.http.client.{Response, AsyncHttpClient}
import java.util.concurrent.{Future, TimeUnit}
import org.json4s.jackson.Serialization._
import java.net.URLEncoder
import org.json4s._
import org.json4s.jackson.JsonMethods._


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
        val databases = read[NewResponse](r._1).results.flatMap(result => {
          result.series.flatMap(series => {
            series.values.flatMap(value => {
              value.map(v => {
                new response.Database(v)
              })
            })
          })
        })
        (databases, None)
      case Some(err) => (Nil, Some(err))
    }
  }

  def createUser(username: String, password: String, isAdmin: Boolean): error.Error = {
    val privs = if (isAdmin) " WITH ALL PRIVILEGES" else ""
    val q = s"CREATE USER $username WITH PASSWORD '$password'$privs"
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

  def query(query: String, timePrecision: Option[String] = None, chunked: Boolean = false): (response.Response, error.Error) = {
    try {
      val q = URLEncoder.encode(query, "UTF-8")
      val url = getUrl(s"/db/$database/series") + s"&q=$q&chunked=$chunked" +
        (if (timePrecision.isDefined) s"&time_precision=${timePrecision.get}" else "")

      val r = getResponse(httpClient.prepareGet(url).execute())
      responseToError(r) match {
        case None => (response.Response(r.getResponseBody), None)
        case Some(err) => (null, Some(err))
      }
    } catch {
      case ex: Exception => (null, Some(ex.getMessage))
    }
  }

  def getContinuousQueries: (List[response.ContinuousQuery], error.Error) = {
    try {
      val url = getUrl(s"/db/$database/continuous_queries")
      val r = getResponse(httpClient.prepareGet(url).execute())
      responseToError(r) match {
        case None => (read[List[response.ContinuousQuery]](r.getResponseBody), None)
        case Some(err) => (Nil, Some(err))
      }
    } catch {
      case ex: Exception => (Nil, Some(ex.getMessage))
    }
  }

  def deleteContinuousQueries(id: Int): error.Error = {
    try {
      val url = getUrl(s"/db/$database/continuous_queries/$id")
      responseToError(getResponse(httpClient.prepareDelete(url).execute()))
    } catch {
      case ex: Exception => Some(ex.getMessage)
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

  private def query(query: String): (String, error.Error) = {
    try {
      val url = getUrl(s"/query") + "&" + createQueryString(Map("q" -> query))
      val r = getResponse(httpClient.prepareGet(url).execute())
      (r.getResponseBody, responseToError(r))
    } catch {
      case ex: Exception => (null, Some(ex.getMessage))
    }
  }

  private def queryDatabase(query: String): (String, error.Error) = {
    try {
      val url = getUrl(s"/query") + "&" + createQueryString(Map("db" -> database, "q" -> query))
      val r = getResponse(httpClient.prepareGet(url).execute())
      (r.getResponseBody, responseToError(r))
    } catch {
      case ex: Exception => (null, Some(ex.getMessage))
    }
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
