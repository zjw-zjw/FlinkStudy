package com.zjw.flink1205.apitest

import com.zjw.flnk1205.function.MyWindowFunc
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

object WatermarkTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)   // 设置时间语义

    val inputStream: DataStream[String] = env.socketTextStream("node01", 7777)


    val dataStream: DataStream[SensorReading] = inputStream
      .map( data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      } )
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1L)) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
      }) // 定义watermark
//      .assignTimestampsAndWatermarks( new MyWMAssigner(1000L) )


    val windowStream: DataStream[(String, Long, Long, Int)] = dataStream
      .keyBy("id")
      .timeWindow(Time.seconds(15L))
//      .allowedLateness(Time.minutes(1L))
//      .sideOutputLateData( new OutputTag[SensorReading]("late") )
      .apply( new MyWindowFunc() )


    dataStream.print("data")
    windowStream.print("window result")

//    windowStream.getSideOutput( new OutputTag[SensorReading]("late") )


    env.execute("water mark")
  }
}


// 自定义 watermark (周期性生成)
class MyWMAssigner(lateness: Long) extends AssignerWithPeriodicWatermarks[SensorReading]{
  // 需要两个关键参数，延迟时间 和 当前所有数据的最大时间戳
//  val lateness: Long = 1000L
  var maxTs: Long = Long.MinValue + lateness

  override def getCurrentWatermark: Watermark = {
    new Watermark(maxTs - lateness)
  }

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    maxTs = maxTs.max(element.timestamp)
    element.timestamp * 1000L
  }
}


// 自定义一个断点式生成 watermark的Assigner
class MyWMAssigner2 extends AssignerWithPunctuatedWatermarks[SensorReading]{
  val lateness: Long = Long.MinValue

  override def checkAndGetNextWatermark(lastElement: SensorReading, extractedTimestamp: Long): Watermark = {
    if ( lastElement.id == "sensor_1" ) {
      new Watermark(extractedTimestamp - lateness)
    } else {
      null
    }
  }

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    element.timestamp * 1000L
  }
}