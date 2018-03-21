package xyz.bytzdev.tp.util

import java.io.{File, FileInputStream, InputStream}
import java.sql.Connection
import java.util.Properties
import org.slf4j.LoggerFactory
import org.slf4j.Logger

import com.mchange.v2.c3p0.ComboPooledDataSource

class MDBManager() extends Serializable{
  private val cpds: ComboPooledDataSource = new ComboPooledDataSource(true)
  private val prop = new Properties()
  private var in:InputStream = getClass().getResourceAsStream("/c3p0.properties")
  private val logger: Logger = LoggerFactory.getLogger(classOf[MDBManager])

  try {
    logger.info("======== Start Get DB Info   ==============")

    prop.load(in)
    cpds.setJdbcUrl(prop.getProperty("jdbcUrl"))
    cpds.setDriverClass(prop.getProperty("driverClass"))
    cpds.setUser(prop.getProperty("user"))
    cpds.setPassword(prop.getProperty("password"))
    cpds.setMaxPoolSize(Integer.valueOf(prop.getProperty("maxPoolSize")))
    cpds.setMinPoolSize(Integer.valueOf(prop.getProperty("minPoolSize")))
    cpds.setAcquireIncrement(Integer.valueOf(prop.getProperty("acquireIncrement")))
    cpds.setInitialPoolSize(Integer.valueOf(prop.getProperty("initialPoolSize")))
    cpds.setMaxIdleTime(Integer.valueOf(prop.getProperty("maxIdleTime")))


    logger.info(prop.getProperty("jdbcUrl"))
  } catch {
    case ex: Exception => {
      logger.error("An error occurred", ex)
      ex.printStackTrace()
    }
  }
  def getConnection:Connection={
    try {
      cpds.getConnection()
    } catch {
      case ex:Exception => ex.printStackTrace()
        null
    }
  }
}
object MDBManager {
  var mdbManager: MDBManager = _

  def getMDBManager: MDBManager = {
    synchronized {
      if (mdbManager == null) {
        mdbManager = new MDBManager()
      }
    }
    mdbManager
  }
}