package com.zjw.flnk1205.function

import com.zjw.flink1205.apitest.SensorReading
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration


// 自定义一个 RichMapFunction函数类
class MyRichMapFunction() extends RichMapFunction[SensorReading, String] {

  // 只调用一次  适用于 初始化 操作
  override def open(parameters: Configuration): Unit = super.open(parameters)


  override def close(): Unit = super.close()



  // 每来一个元素就调用一次
  override def map(value: SensorReading): String = value.id

}