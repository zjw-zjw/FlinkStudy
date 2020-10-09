package com.zjw.flink1205.table

import com.zjw.flink1205.apitest.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

object OutputTableTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 读取数据创建DataStream
//    val inputStream: DataStream[String] = env.socketTextStream("localhost", 7777)
    val inputStream = env.readTextFile("D:\\IdeaProjects\\flink1205\\src\\data\\sensor.txt")

    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
      val splitStr = data.split(",")
      SensorReading(splitStr(0), splitStr(1).toLong, splitStr(2).toDouble)
    })

    // 创建 表执行环境
    val envSettings = EnvironmentSettings.newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings = envSettings)

    // 将dataStream转换成table
    val sensorTable = tableEnv.fromDataStream(dataStream, 'id, 'temperature as 'temp, 'timestamp as 'ts)


    // 对table进行转换操作
    val aggResultTable = sensorTable
      .groupBy('id)
      .select('id, 'id.count as 'cnt)

    // 定义一张 输出表，这就是要写入数据的tableSink
    tableEnv.connect( new FileSystem().path("D:\\IdeaProjects\\flink1205\\src\\data\\out.txt") )
      .withFormat( new Csv())
      .withSchema( new Schema()
          .field("id", DataTypes.STRING())
          .field("temp", DataTypes.DOUBLE())
          .field("ts", DataTypes.BIGINT())
      )
      .createTemporaryTable("outputTable")

    sensorTable.insertInto("outputTable")

    sensorTable.toAppendStream[(String, Double, Long)].print("")



    env.execute("output table test")


  }
}
