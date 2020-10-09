package com.zjw.flink1205.apitest.sinktest

import java.util.Properties

import com.zjw.flink1205.apitest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink.RowFormatBuilder
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

object KafkaSinkTest {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

//    val inputStream = env.readTextFile("D:\\IdeaProjects\\flink1205\\src\\data\\sensor.txt")

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "node01:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    val inputStream: DataStream[String] = env.addSource( new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))

    // 1、基本转换操作
    val dataStream: DataStream[String] = inputStream
      .map( data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble).toString
      })


//    dataStream.print()

//    dataStream.writeAsCsv("D:\\IdeaProjects\\flink1205\\src\\data\\out.txt")

//    dataStream.addSink( new StreamingFileSink[String]())

    inputStream.print("Kafka sensor topic")

    dataStream.addSink(
      new FlinkKafkaProducer011[String]("node01:9092", "sink-test", new SimpleStringSchema())
    )


    env.execute("kafka sink test")

  }
}
