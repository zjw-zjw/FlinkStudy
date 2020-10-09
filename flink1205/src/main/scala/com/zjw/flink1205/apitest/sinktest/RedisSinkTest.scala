package com.zjw.flink1205.apitest.sinktest

import java.util.Properties

import com.zjw.flink1205.apitest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object RedisSinkTest {
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
    val dataStream: DataStream[SensorReading] = inputStream
      .map( data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })


    // 定义 Redis配置类
    val jedisPoolConfig: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
      .setHost("node01")
      .setPort(6379)
      .build()

    // 定义一个 RedisMapper
    val myMapper = new RedisMapper[SensorReading] {

      // 定义到保存数据到 Redis 的命令   hset table_name key value
      override def getCommandDescription: RedisCommandDescription = {
        new RedisCommandDescription( RedisCommand.HSET, "sensor_temp")
      }

      override def getValueFromData(data: SensorReading): String = data.id

      override def getKeyFromData(data: SensorReading): String = data.temperature.toString
    }

    // sink 到 Redis
    dataStream.addSink(
      new RedisSink[SensorReading](jedisPoolConfig, myMapper)
    )

    env.execute("redis sink test")
  }
}
