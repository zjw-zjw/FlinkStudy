package com.zjw.flink1205.apitest

import java.util.Properties

import com.zjw.flnk1205.function.MyWindowFunc
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.util.Collector

object WindowTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

//    val inputStreamFromFile: DataStream[String] = env.readTextFile("D:\\IdeaProjects\\flink1205\\src\\data\\sensor.txt")

    // 4. 从Kafka读取数据
//    val properties = new Properties()
//    properties.setProperty("bootstrap.servers", "node01:9092")
//    properties.setProperty("group.id", "consumer-group")
//    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
//    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
//    properties.setProperty("auto.offset.reset", "latest")
//    val inputStream: DataStream[String] = env.addSource( new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))

    val inputStream = env.socketTextStream("node01", 7777)

    // 1、基本转换操作
    val dataStream: DataStream[SensorReading] = inputStream
      .map( data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })

    val resultStream = dataStream
      .keyBy("id")
//      .window( EventTimeSessionWindows.withGap(Time.minutes(1L)) )  // 会话窗口
//        .timeWindow(Time.seconds(15L), Time.seconds(5L))   // 滑动窗口
      .window( TumblingProcessingTimeWindows.of(Time.hours(1L), Time.hours(-8)) ) // 滚动窗口
//      .reduce( (curRes, newData) => {
//      // 聚合出每个Sensor的最大时间戳和最小温度值
//      SensorReading(curRes.id, curRes.timestamp.max(newData.timestamp), curRes.temperature.min(newData.temperature))
//    })
      .apply(new MyWindowFunc())


    dataStream.print("data")
    resultStream.print("result")


    env.execute("window test")
  }
}

