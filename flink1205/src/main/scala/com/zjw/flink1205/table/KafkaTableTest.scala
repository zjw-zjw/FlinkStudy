package com.zjw.flink1205.table

import com.zjw.flink1205.apitest.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}

object KafkaTableTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


    // 创建 表执行环境
    val envSettings = EnvironmentSettings.newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings = envSettings)


    // 从kafak 读取数据
    tableEnv.connect( new Kafka()
        .version("0.11")
        .topic("sensor")
        .property("bootstrap.servers", "node01:9092")
        .property("zookeeper.connect", "node01:2181")
    )
      .withFormat( new Csv())
      .withSchema( new Schema()
          .field("id", DataTypes.STRING())
          .field("timestamp", DataTypes.BIGINT())
          .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("kafkaInputTable")

    // 做转换操作
    val sensorTable: Table = tableEnv.from("kafkaInputTable")
    val resultTable = sensorTable
      .select('id, 'temperature)
      .filter('id === "sensor_1")

    val aggResultTable: Table = sensorTable
      .groupBy('id)
      .select('id, 'id.count as 'cnt)

    // 定义一个连接到kafka的输出表
    tableEnv.connect( new Kafka()
      .version("0.11")
      .topic("sinkTest")
      .property("bootstrap.servers", "node01:9092")
      .property("zookeeper.connect", "node01:2181")
    )
      .withFormat( new Csv() )
      .withSchema( new Schema()
        .field("id", DataTypes.STRING())
        .field("cnt", DataTypes.BIGINT())
      )
      .createTemporaryTable("kafkaOutputTable")

    // 将结果表输出
    resultTable.insertInto("kafkaOutputTable")
//    aggResultTable.into

    env.execute("kafka table test job")
  }
}
