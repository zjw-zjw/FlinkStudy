package com.zjw.flink1205.table

import com.zjw.flink1205.apitest.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._

object TableExample {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

//    val inputStream: DataStream[String] = env.readTextFile("D:\\IdeaProjects\\flink1205\\src\\data\\sensor.txt")
    val inputStream: DataStream[String] = env.socketTextStream("localhost", 7777)

    val dataStream: DataStream[SensorReading] = inputStream
      .map( data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      } )

    // 创建表执行环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // 基于数据流，转换成一张表，然后进行操作
    val dataTable: Table = tableEnv.fromDataStream(dataStream)

    // 调用Table API 得到转换结果
    val resultTable: Table = dataTable
      .select("id, temperature")
      .filter("id == 'sensor_1'")

    // 或者直接写SQL 得到转换结果
    val resuluSqlTable: Table = tableEnv.sqlQuery("select id, temperature from " + dataTable + " where id = 'sensor_1'")


    // 转换回数据流，打印输出
    val resultStream:DataStream[(String, Double)] = resultTable.toAppendStream[(String, Double)]
    val resultSqlStream: DataStream[(String, Double)] = resuluSqlTable.toAppendStream[(String, Double)]

    resultStream.print("result")
    resultSqlStream.print("sql result")

    resultTable.printSchema()

    env.execute("table example job")
  }
}
