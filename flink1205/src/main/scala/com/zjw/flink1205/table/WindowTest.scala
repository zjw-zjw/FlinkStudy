package com.zjw.flink1205.table

import com.zjw.flink1205.apitest.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object WindowTest {
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

    // 窗口操作
    // 1. group 窗口， 开一个10秒的滚动窗口， 统计每个传感器温度的数量
    val groupResultTable: Table = sensorTable
      .window( Tumble over 10.seconds on 'et as 'tw )
      .groupBy('id, 'tw)
      .select('id, 'id.count, 'tw.end)


    // 2. Group窗口 SQL实现
    tableEnv.createTemporaryView("sensor", sensorTable)
    val groupResultSqlTable: Table = tableEnv.sqlQuery(
      """
        |select
        |   id,
        |   count(id) as cnt,
        |   tumble_end(et, interval '10' second)
        | from
        |     sensor
        | group by
        |     id,
        |     tumble(et, interval '10' second)
      """.stripMargin)



    // 转化成流打印输出
//    groupResultTable.toAppendStream[Row].print("")
    groupResultSqlTable.toAppendStream[Row].print("sql result")

    env.execute("table window test job")
  }
}
