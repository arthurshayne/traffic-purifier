package xyz.bytzdev.tp.util


import java.io.File

import com.maxmind.geoip2.DatabaseReader
import java.net.InetAddress
import java.sql.Date

class Ip2Geo {


}

object Ip2Geo {
  val database = new File("/usr/local/geoip2/GeoLite2-City.mmdb")

  val reader: DatabaseReader = new DatabaseReader.Builder(database).build


  def getGeoInfo (ip: String) : String = {
    try {
      val response = reader.city(InetAddress.getByName(ip))

      val country = response.getCountry
      val subdivision = response.getMostSpecificSubdivision

      s"${country.getNames.get("zh-CN")} - ${subdivision.getNames.get("zh-CN")}"
    } catch {
      case e: Exception => "未知"
    }
  }


  def main(args: Array[String]): Unit = {
    println(getGeoInfo("117.11.200.73"))
  }
}