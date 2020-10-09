package com.zjw.flink1205.table

import com.zjw.flink1205.apitest.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

object TableFunctionTest {
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

    // 创建一个UDF实例
    val split = new Split("_")
    // 调用Table API, TableFunction使用的时候， 需要用 joinLateral方法
    val resultTable = sensorTable
      .joinLateral( split('id) as ('word, 'length) )
      .select('id, 'ts, 'word, 'length)

    // SQL 实现
    tableEnv.createTemporaryView("sensor", sensorTable)
    tableEnv.registerFunction("split", split)

    val resultSqlTable = tableEnv.sqlQuery(
      """
        |select id, ts, word, length
        |from
        | sensor,
        | lateral table(split(id)) as splitid(word, length)
      """.stripMargin)

//    resultTable.toAppendStream[Row].print("result")
    resultSqlTable.toAppendStream[Row].print("sql result")

    env.execute("scalar function test")
  }
}


// 实现自定义的 Table Function
// 对一个 String，用某个 分隔符 切分之后的(word, word_length)
class Split(separator:String) extends TableFunction[(String, Int)] {

  def eval(str: String): Unit = {
    str.split(separator).foreach(
      word => collect((word, word.length))
    )
  }
}
