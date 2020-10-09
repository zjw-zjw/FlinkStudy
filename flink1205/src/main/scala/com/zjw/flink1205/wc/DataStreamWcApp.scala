package com.zjw.flink1205.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

// 流处理
object DataStreamWcApp {

  def main(args: Array[String]): Unit = {


    // 创建流处理环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

//    env.disableOperatorChaining()

    // 从程序运行参数中读取 hostname和port
    val params: ParameterTool = ParameterTool.fromArgs(args)
    val hostname: String = params.get("host")
    val port: Int = params.getInt("port")

    // 接收socket文本流
    val sourceDs: DataStream[String] = env.socketTextStream(hostname, port)

    val resultDs: DataStream[(String, Int)] = sourceDs
      .flatMap(_.split(" "))
      .filter(_.nonEmpty).slotSharingGroup("1")
      .map((_, 1)).disableChaining()
      .keyBy(0)   // 按照第一个元素分组
      .sum(1)     // 按照第二个元素求和

    resultDs.print()

    env.execute("stream word count job")
  }

}
