package com.zjw.flnk1205.function

import com.zjw.flink1205.apitest.SensorReading
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.util.Collector

class TempChangeWarningWithFlatmap(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)]{

  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState( new ValueStateDescriptor[Double]("last-temp", classOf[Double]) )


  override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
    // 从状态中取出上一次温度值
    val lastTemp = lastTempState.value()

    // 更新状态
    lastTempState.update(value.temperature)

    // 跟当前温度值 计算差值， 跟阈值比较，如果大于就报警
    val diff = (value.temperature - lastTemp).abs
    if (diff > threshold) {
      out.collect( (value.id, lastTemp, value.temperature) )
    }
  }
}
