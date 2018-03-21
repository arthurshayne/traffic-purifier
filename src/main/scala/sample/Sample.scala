/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sample

import java.text.SimpleDateFormat

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object Sample {
  def main(args: Array[String]) {
    // the port to connect to
    val port: Int = try {
      ParameterTool.fromArgs(args).getInt("port")
    } catch {
      case e: Exception => {
        System.err.println("No port specified. Please run 'SocketWindowWordCount --port <port>'")
        return
      }
    }

    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // get input data by connecting to the socket
    val text = env.socketTextStream("localhost", port, '\n')

    val lateOutputTag = OutputTag[Traffic]("late-data")

    // parse the data, group it, window it, and aggregate the counts
    val resultStream = text
      .map { w => {
        val ws = w.split(",")

        Traffic(new SimpleDateFormat("yyyyMMddHHmmss").parse(ws(0)).getTime, ws(1), ws(2))
      }}
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Traffic](Time.seconds(5)) {
        override def extractTimestamp(element: Traffic): Long = {
          // val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          // println(s"timestamp:${element.timestamp}|${formatter.format(element.timestamp)}, ip: ${element.ip}, url: ${element.url}, wm: ${this.getCurrentWatermark.getTimestamp}|${formatter.format(this.getCurrentWatermark.getTimestamp)}")
          element.timestamp
        }
      })
      .keyBy("url")
      .timeWindow(Time.seconds(5))
      .sideOutputLateData(lateOutputTag)
      .sum("count")

    // print the results with a single thread, rather than in parallel
    resultStream.print().setParallelism(1)

    resultStream
      .getSideOutput(lateOutputTag)
      .map(p => {
        p.timestamp = (p.timestamp / (60 * 1000)) * (60 * 1000)
        p.status = "drop"
        p})
      .keyBy("timestamp", "url")
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .sum("count")
      .print().setParallelism(1)

    env.execute("Url Statistic")
  }

  // Data type for words with count
  case class Traffic(var timestamp: Long, ip: String, url: String, count: Long = 1, var status: String = "normal")
}
