package xyz.bytzdev.tp.statistics

import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.{SimpleStringSchema, TypeInformationSerializationSchema}
import org.apache.flink.api.java.typeutils.TypeInfoParser
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.util.Collector
import xyz.bytzdev.tp.util.Ip2Geo

object RequestStatistic {
  def main(args: Array[String]) {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.56.101:9092")

    val myConsumer = new FlinkKafkaConsumer011[String]("traffic-request", new SimpleStringSchema(), properties)

    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // get input data by connecting to the socket
    val text = env.addSource(myConsumer)

    // parse the data, group it, window it, and aggregate the counts
    val baseStream = text
      .map { w => {
        val ws = w.split(",")

        Traffic(new SimpleDateFormat("yyyyMMddHHmmss").parse(ws(0)).getTime, ws(1), Ip2Geo.getGeoInfo(ws(1)), ws(2))
      }}
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Traffic](Time.seconds(10)) {
        override def extractTimestamp(element: Traffic): Long = element.timestamp
      })

    val urlStream = baseStream
      .keyBy(_.url)
      .timeWindow(Time.seconds(60))
      .reduce(
        (r1: Traffic, r2: Traffic) => { Traffic(r1.timestamp, r1.ip, r1.geo, r1.url, r1.count + r2.count) },
        ( key: String,
          window: TimeWindow,
          minReadings: Iterable[Traffic],
          out: Collector[UrlResult] ) =>
        {
          val min = minReadings.iterator.next()
          out.collect(UrlResult(window.getStart, window.getEnd, key, min.count))
        }
      )

    val geoStream = baseStream
      .keyBy(_.geo)
      .timeWindow(Time.seconds(60))
      .reduce(
        (r1: Traffic, r2: Traffic) => { Traffic(r1.timestamp, r1.ip, r1.geo, r1.url, r1.count + r2.count) },
        ( key: String,
          window: TimeWindow,
          minReadings: Iterable[Traffic],
          out: Collector[GeoResult] ) =>
        {
          val min = minReadings.iterator.next()
          out.collect(GeoResult(window.getStart, window.getEnd, key, min.count))
        }
      )

//    val urlProducer = new FlinkKafkaProducer011[UrlResult](
//      "192.168.56.101:9092",       // broker list
//      "url-statistic",                  // target topic
//      new TypeInformationSerializationSchema[UrlResult](TypeInfoParser.parse("xyz.bytzdev.tp.statistics.UrlResult"), env.getConfig));   // serialization schema

     urlStream.addSink(new Url1MinSinkToMySql)


//    val geoProducer = new FlinkKafkaProducer011[GeoResult](
//      "192.168.56.101:9092",       // broker list
//      "geo-statistic",                  // target topic
//      new TypeInformationSerializationSchema[GeoResult](TypeInfoParser.parse("xyz.bytzdev.tp.statistics.GeoResult"), env.getConfig));   // serialization schema


    geoStream.addSink(new Geo1MinSinkToMySql)

    env.execute("Request Statistic")
  }

}


// Data type for words with count
case class Traffic(var timestamp: Long, ip: String, geo: String, url: String, count: Long = 1)

case class UrlResult(beginTimestamp: Long, endTimestamp: Long, url: String, count: Long)

case class GeoResult(beginTimestamp: Long, endTimestamp: Long, geo: String, count: Long)
