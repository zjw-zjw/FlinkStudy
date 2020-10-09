package com.zjw.flink1205.apitest.sinktest

import java.util
import java.util.Properties

import com.zjw.flink1205.apitest.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

object EsSinkTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream = env.readTextFile("D:\\IdeaProjects\\flink1205\\src\\data\\sensor.txt")


    // 1、基本转换操作
    val dataStream: DataStream[SensorReading] = inputStream
      .map( data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })


    // 定义 HttpHosts
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("node01", 9200))

    // 定义一个 elasticsearchSinkFunction
    val esSinkFunc = new ElasticsearchSinkFunction[SensorReading] {
      override def process(element: SensorReading, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
        // 包装写入 es的数据
        val dataSource = new util.HashMap[String, String]()
        dataSource.put("sensor_id", element.id)
        dataSource.put("temperature", element.temperature.toString)
        dataSource.put("ts", element.timestamp.toString)
        // 创建一个 indexRequest
        val idnexRequest: IndexRequest = Requests.indexRequest()
          .index("sensor_temp")
          .`type`("readingdata")
          .source(dataSource)
        // 用 Idexer 发送 Http请求
        indexer.add( idnexRequest )
        println( element + "saved successfully" )
      }
    }

    // sink 到 es
    dataStream.addSink( new ElasticsearchSink.Builder[SensorReading](httpHosts, esSinkFunc).build())

    env.execute("es sink test")
  }
}
