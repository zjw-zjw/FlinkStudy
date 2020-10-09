package com.zjw.flink1205.apitest.sinktest

import java.sql.{Connection, Driver, DriverManager, PreparedStatement}

import com.zjw.flink1205.apitest.{MySensorSource, SensorReading}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

object MysqlSinkTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream = env.readTextFile("D:\\IdeaProjects\\flink1205\\src\\data\\sensor.txt")

//    val dataStream: DataStream[SensorReading] = env.addSource( new MySensorSource())

//     1、基本转换操作
    val dataStream: DataStream[SensorReading] = inputStream
      .map( data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })

    dataStream.addSink( new MysqlSink())

    env.execute("mysql sink test")
  }
}


// 自定一个 sinkFunction
class MysqlSink extends RichSinkFunction[SensorReading]{

  // 定义 连接、 预编译语句
  var conn : Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  // 在 open 生命周期方法中创建连接和预编译语句
  override def open(parameters: Configuration): Unit = {
//    Class.forName("com.mysql.jdbc.Driver")
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123")
    insertStmt = conn.prepareStatement("insert into temp (sensor, temperature) value(?,?)")
    updateStmt = conn.prepareStatement("update temp set temperature = ? where sensor = ?")
  }

  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    // 执行更新语句
    updateStmt.setDouble(1, value.temperature)
    updateStmt.setString(2, value.id)
    updateStmt.execute()
    // 如果刚才没有更新数据 那么执行插入操作
    if (updateStmt.getUpdateCount == 0) {
      insertStmt.setString(1, value.id)
      insertStmt.setDouble(2, value.temperature)
      insertStmt.execute()
    }
  }

  // 关闭连接
  override def close(): Unit = {

    if (insertStmt != null) {
      insertStmt.close()
    }
    if (updateStmt != null) {
      updateStmt.close()
    }
    if (conn != null) {
      conn.close()
    }
  }
}