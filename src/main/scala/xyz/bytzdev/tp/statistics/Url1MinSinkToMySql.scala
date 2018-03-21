package xyz.bytzdev.tp.statistics

import java.sql.{Connection, Date, PreparedStatement, Timestamp}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.slf4j.{Logger, LoggerFactory}
import xyz.bytzdev.tp.util.MDBManager

class Url1MinSinkToMySql extends RichSinkFunction[UrlResult] {
  var conn: Connection = null
  var ps: PreparedStatement = null
  val logger: Logger = LoggerFactory.getLogger(classOf[Url1MinSinkToMySql])


  override def open(parameters: Configuration): Unit = {
    conn = MDBManager.getMDBManager.getConnection

    val sql = "insert into url_statistic(begin_time, end_time, url, count) values (?,?,?,?)"
    ps = conn.prepareStatement(sql)
  }


  override def invoke(in: UrlResult): Unit = {
    try {
      ps.setTimestamp(1, new Timestamp(in.beginTimestamp))
      ps.setTimestamp(2, new Timestamp(in.endTimestamp))
      ps.setString(3, in.url)
      ps.setLong(4, in.count)

      ps.executeUpdate()

    } catch {
      case e: Exception => logger.error("An error occurred", e)
    }
  }

  override def close(): Unit = {
    if (ps != null) {
      ps.close()
    }
    if (conn != null) {
      conn.close()
    }
  }
}
