package com.zjw.flink1205.apitest

import com.zjw.flnk1205.function.MyRichMapFunction
import org.apache.flink.api.common.functions.{FilterFunction, RichFilterFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

object TransformTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStreamFromFile: DataStream[String] = env.readTextFile("D:\\IdeaProjects\\flink1205\\src\\data\\sensor.txt")

    // 1、基本转换操作
    val dataStream: DataStream[SensorReading] = inputStreamFromFile
      .map( data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })

    // 2.滚动聚合操作
    val aggStream: DataStream[SensorReading] = dataStream
      .keyBy("id")
      .sum("temperature")

    // 3.复杂聚合操作
    val reduceStream: DataStream[SensorReading] = dataStream
      .keyBy("id")
      .reduce((curRes, newData) => {
        // 聚合出每个Sensor的最大时间戳和最小温度值
        SensorReading(curRes.id, curRes.timestamp.max(newData.timestamp), curRes.temperature.min(newData.temperature))
      })


    // 4.分流操作   split/select
    val splitStream: SplitStream[SensorReading] = dataStream
      .split(data => {
        if (data.temperature > 35) {
          Seq("high")
        }
        else {
          Seq("low")
        }
      })

    val highTempStream: DataStream[SensorReading] = splitStream.select("high")
    val lowTempStream: DataStream[SensorReading] = splitStream.select("low")
    val allTempStream: DataStream[SensorReading] = splitStream.select("high", "low")


    // 5. 合流操作
    val highWarningStream: DataStream[(String, Double)] = highTempStream.map(data => {
      (data.id, data.temperature)
    })

    val connectedStream: ConnectedStreams[(String, Double), SensorReading] = highWarningStream.connect(lowTempStream)

    val coMapStream: DataStream[(String, Double, String)] = connectedStream.map(
      highWarningData => (highWarningData._1, highWarningData._2, "high temperature warning!"),
      lowTempData => (lowTempData.id, lowTempData.temperature, "normal")
    )

    // union 操作: 可以同时合并多条流
    val unionStream: DataStream[SensorReading] = highTempStream.union(lowTempStream)


    // 6.自定义函数类
//    val filterStream: DataStream[String] = inputStreamFromFile.filter(data => data.startsWith("sensor_1"))
    val filterStream = dataStream.filter( new MyFilter() )

    val myMapStream: DataStream[String] = dataStream.map(new MyRichMapFunction())

//    dataStream.filter(new RichFilterFunction[SensorReading] {
//      override def filter(value: SensorReading): Boolean = {
//        value.id.startsWith("sensor_1")
//      }
//    })


//    dataStream.print()
//    filterStream.print("filter")
//    aggStream.print("agg")
//    reduceStream.print("reduce")
//    highTempStream.print("high")
//    lowTempStream.print("low")
//    allTempStream.print("all")
//    coMapStream.print("coMap")
//    unionStream.print("union")

//    filterStream.print("filter")
    myMapStream.print("map")

    env.execute("transform test")
  }
}


// 自定义一个 Filter函数类
class MyFilter() extends FilterFunction[SensorReading]{
  override def filter(value: SensorReading): Boolean = {
    value.id.startsWith("sensor_1")
  }
}

