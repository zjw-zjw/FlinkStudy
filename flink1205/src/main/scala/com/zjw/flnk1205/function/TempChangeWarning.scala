package com.zjw.flnk1205.function

import com.zjw.flink1205.apitest.SensorReading
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration


// 自定义 RichMapFunction
class TempChangeWarning(threshold: Double) extends RichMapFunction[SensorReading, (String, Double, Double)] {

  // 定义状态变量，上一次的温度值
//  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState( new ValueStateDescriptor[Double]("last-temp", classOf[Double]) )

  var lastTempState: ValueState[Double] = _

  override def open(parameters: Configuration): Unit = {
    lastTempState = getIterationRuntimeContext.getState( new ValueStateDescriptor[Double]("last-temp", classOf[Double]) )
  }


  override def map(value: SensorReading): (String, Double, Double) = {
    // 从状态中取出上一次温度值
    val lastTemp = lastTempState.value()

    // 更新状态
    lastTempState.update(value.temperature)

    // 跟当前温度值 计算差值， 跟阈值比较，如果大于就报警
    val diff = (value.temperature - lastTemp).abs
    if (diff > threshold) {
      (value.id, lastTemp, value.temperature)
    } else {
      ( value.id, 0.0, 0.0 )
    }
  }
}
