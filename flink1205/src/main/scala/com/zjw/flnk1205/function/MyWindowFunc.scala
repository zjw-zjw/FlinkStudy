package com.zjw.flnk1205.function

import com.zjw.flink1205.apitest.SensorReading
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


// 自定义 全窗口函数  类似于批处理
class MyWindowFunc extends WindowFunction[SensorReading, (String, Long, Long, Int), Tuple, TimeWindow]{
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[SensorReading], out: Collector[(String, Long, Long, Int)]): Unit = {
    val id: String = key.asInstanceOf[Tuple1[String]].f0
    out.collect((id, window.getStart, window.getEnd, input.size))
  }
}
