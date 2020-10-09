package com.zjw.flink1205.apitest

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import scala.collection.immutable
import scala.util.Random


// 输入数据的样例类
case class SensorReading(id: String, timestamp: Long, temperature: Double)

object SourceTest {

  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 1. 从集合中读取数据
    val stream1: DataStream[SensorReading] = env.fromCollection(List(
        SensorReading("sensor_1", 1547718944, 35.9),
        SensorReading("sensor_2", 1547718977, 34.6),
        SensorReading("sensor_3", 1547718644, 31.5),
        SensorReading("sensor_4", 1547718961, 35.2),
        SensorReading("sensor_1", 1547718984, 32.9),
        SensorReading("sensor_1", 1547718956, 37.9),
        SensorReading("sensor_1", 1547714544, 15.9)
    ))

    // 2.从文件中读取数据
    val stream2: DataStream[String] = env.readTextFile("D:\\IdeaProjects\\flink1205\\src\\data\\sensor.txt").setParallelism(1)

    val aggDs: DataStream[SensorReading] = stream2.map(item => {
      val list = item.split(",")
      val id = list(0)
      val timestamp = list(1).toLong
      val value = list(2).toDouble
      SensorReading(id, timestamp, value)
    })
      .keyBy(sensor => sensor.id)
      .sum("temperature").setParallelism(1)



    // 3. socket 文本流
//    val socketDs: DataStream[String] = env.socketTextStream(hostname = "node01", port = 7777)


    // 4. 从Kafka读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "node01:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    val kafkaStream: DataStream[String] = env.addSource( new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))


    // 5.自定义Source
    val personSouce: DataStream[SensorReading] = env.addSource(new MySensorSource())

    // 打印输出(sink)
    //    stream2.print("stream2")
    //    aggDs.print().setParallelism(1)
//    kafkaStream.print()

    personSouce.print("personSouce").setParallelism(1)

    env.execute("source test job")
  }
}


// 实现一个自定义的 SourceFunction
// 作用： 自动生成测试数据
class MySensorSource() extends SourceFunction[SensorReading]{

  // 定义一个 flag 表示数据源是否正常运行
  var running: Boolean = true

  override def cancel(): Unit = running = false

  // 随机生成SensorReading数据
  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
    // 定义一个随机数发生器
    val rand = new Random()

    // 随机生成10个Sensor的温度值， 并且不停在之前温度值的基础上更新（随机上下波动）
    // 首先生成10个传感器的初始温度
    var curTemps: immutable.Seq[(String, Double)] = 1.to(10).map(
      i => ("sensor_" + i, 60 + rand.nextGaussian() * 20)
    )

    // 无限循环生成随机数据流
    while (running) {
      // 在当前温度基础上随机生成微小波动
      curTemps = curTemps.map(
        data => (data._1, data._2 + rand.nextGaussian())
      )

      // 获取当前系统时间戳
      val curTs = System.currentTimeMillis()

      // 包装成样例类，用ctx发出数据
      curTemps.foreach(
        data => ctx.collect(SensorReading(data._1, curTs, data._2))
      )

      // 定义间隔时间
      Thread.sleep(1000L)
    }
  }
}
