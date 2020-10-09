package com.zjw.flink1205.table

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table, TableEnvironment}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors._

object TableApITest {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 创建表的执行环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    /*// 1.1 老版本的planner的流式查询
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useOldPlanner()     // 用捞版本
      .inStreamingMode()   // 流处理模式
      .build()
    val oldStreamTableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    // 1.2 老版本的批处理环境
    val batchEnv: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val batchTableEnv: BatchTableEnvironment = BatchTableEnvironment.create(batchEnv)

    // 1.3 blink 版本的  流式查询
    val bsSettings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()  // 使用blink模式
      .inStreamingMode()
      .build()
    val bsBatchEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, bsSettings)

    // 1.4 blink 版本的 批式查询
    val bbSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inBatchMode()
      .build()
    val bbTableEnv: TableEnvironment = TableEnvironment.create(bbSettings)*/


    // 2，连接外部系统,读取数据
    val filePath = "D:\\IdeaProjects\\flink1205\\src\\data\\sensor.txt"

    tableEnv.connect(new FileSystem().path(filePath))
      .withFormat(new OldCsv())   // 定义从外部文件读取数据之后的格式化方法
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
    ) // 定义表结构
      .createTemporaryTable("inputTable")  // 在表环境注册一张表

    // 2.2 消费Kafka的数据
//    tableEnv.connect(new Kafka()
//        .version("0.11") // 定义版本
//        .topic("sensor")
//        .property("zookeeper.connect", "node01:2181")
//        .property("bootstrap.servers", "node01:9092")
//    )
//      .withFormat(new Csv())
//      .withSchema(new Schema()
//        .field("id", DataTypes.STRING())
//        .field("timestamp", DataTypes.BIGINT())
//        .field("temperature", DataTypes.DOUBLE())
//      )
//      .createTemporaryTable("kafkaInputTable")


    // 3. 表的查询转换
    val sensorTable = tableEnv.from("inputTable")
    // 3.1 简单的转换
    val resultTable: Table = sensorTable
      .select('id, 'temperature)
      .filter('id === "sensor_1")

    // sql 方式
    val resultSqlTable: Table = tableEnv.sqlQuery(
      """
        |select id, temperature
        |from inputTable
        |where id = 'sensor_1'
      """.stripMargin)


    // 3.2 聚合转换, 统计每个传感器温度个数
    val aggResultTable: Table = sensorTable
      .groupBy('id)
      .select('id, 'id.count as 'id_count)

//    val sqlResultTable: Table = tableEnv.sqlQuery("select id, count(id) as cnt from inputTable group by id")

    // 测试输出
//    resultTable.toAppendStream[(String, Double)].print("result")
    aggResultTable.toRetractStream[(String, Long)].print("agg")
//    resultSqlTable.toRetractStream[(String, Long)].print("sql Result")

    env.execute("table api test job")
  }
}
