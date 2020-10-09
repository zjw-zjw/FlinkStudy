package com.zjw.flink1205.table

import com.zjw.flink1205.apitest.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

object ScalarFunctionTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream = env.readTextFile("D:\\IdeaProjects\\flink1205\\src\\data\\sensor.txt")
    val dataStream = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
      })

    // 创建表执行环境
    val settings = EnvironmentSettings
      .newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    // 将dataStram转换为 Table
    val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'timestamp as 'ts, 'temperature as 'temp, 'et.rowtime)

    // 创建一个 UDF 的实例
    val hashCode = new HashCode(10)
    // 调用Table API
    val resultTable = sensorTable
      .select( 'id, 'ts, hashCode('id) )

    // SQL 实现
    tableEnv.createTemporaryView("sensor", sensorTable)
    tableEnv.registerFunction("hashcode", hashCode)
    val resultSqlTable = tableEnv.sqlQuery(
      """
        |select
        |   id,
        |   ts,
        |   hashcode(id)
        |from
        |   sensor
      """.stripMargin)

    resultTable.toAppendStream[Row].print("result")
    resultSqlTable.toAppendStream[Row].print("sql result")

    env.execute("scalar function test")
  }
}



// 自定义标量函数
class HashCode( factor: Int ) extends ScalarFunction {
  // 必须要实现一个 eval方法，它的参数是当前传入的字段，它输出的是一个Int类型的哈希值
  def eval(str: String): Int = {
    return str.hashCode * factor
  }
}


// 自定义 聚合函数
