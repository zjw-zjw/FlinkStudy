package com.zjw.flink1205.apitest

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object SideOutputTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream: DataStream[String] = env.socketTextStream("node01", 7777)

    val dataStream: DataStream[SensorReading] = inputStream
      .map( data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      } )

    // 用processFunction的 侧输出流实现 分流操作
    val highTempStream = dataStream
      .process( new SplitTempProcessor(30.0d) )

    val lowTempStream = highTempStream.getSideOutput( new OutputTag[(String, Long, Double)]("low-temp") )

    // 打印输出
    highTempStream.print("high")
    lowTempStream.print("low")

    env.execute("side output job")
  }
}

// 自定义processFunction，用于区分高低 温度的数据
class SplitTempProcessor(threshold: Double) extends ProcessFunction[SensorReading, SensorReading]{

  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
      // 判断当前数据的温度值 如果大于 阈值，输出到主流，如果小于阈值，输出到侧输出流
    if ( value.temperature > threshold ) {
      out.collect(value)
    } else {
      ctx.output( new OutputTag[(String, Long, Double)]("low-temp"), (value.id, value.timestamp, value.temperature) )
    }
  }
}
