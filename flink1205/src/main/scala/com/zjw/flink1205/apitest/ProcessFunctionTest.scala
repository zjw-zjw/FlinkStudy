package com.zjw.flink1205.apitest

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object ProcessFunctionTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream = env.socketTextStream("node01", 7777)

    val dataStream: DataStream[SensorReading] = inputStream
      .map( data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      } )

    // 检测每一个传感器的温度是否连续上升，在10秒之内
    val warningStream: DataStream[String] = dataStream
      .keyBy("id")
      .process( new TempIncreWarning(10000L) )

    warningStream.print()

    env.execute("process Function job")
  }
}

// 自定义 KeyedProcessFunction
class TempIncreWarning(interval: Long) extends KeyedProcessFunction[Tuple, SensorReading, String]{

  // 由于需要跟之前的温度值作对比，所以将上一个温度保存成状态
  lazy val lastTempSate: ValueState[Double] = getRuntimeContext.getState( new ValueStateDescriptor[Double]("lastTemp", classOf[Double]) )
  // 为了方便删除定时器，还需要保存定时器的时间戳
  lazy val curTimerTsState: ValueState[Long] = getRuntimeContext.getState( new ValueStateDescriptor[Long]("curTimerTs", classOf[Long]) )


  // 每来一个元素，调用一次
  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#Context, out: Collector[String]): Unit = {
    // 取出状态
    val lastTemp = lastTempSate.value()
    val curTimerTs = curTimerTsState.value()

    // 将上次温度值的状态更新为当前数据的温度值
    lastTempSate.update(value.temperature)

    // 判断当前温度值，如果比之前温度高，并且没有定时器的话(默认值为0)，就注册10s后的定时器
    if ( value.temperature > lastTemp && curTimerTs == 0 ) {
      val ts = ctx.timerService().currentProcessingTime() + interval
      ctx.timerService().registerProcessingTimeTimer(ts)    // 注册定时器
      curTimerTsState.update(ts)  // 更新状态
    }

    // 如果温度下降，那么删除定时器
    else if ( value.temperature < lastTemp ) {
      ctx.timerService().deleteProcessingTimeTimer(curTimerTs)
      // 清空状态
      curTimerTsState.clear()
    }

  }

  // 定时器触发，说明10s内没有来下降的温度值。直接报警
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val curTs: String = sdf.format(new Date(System.currentTimeMillis()))
    val keyName: String = ctx.getCurrentKey.getField[String](0)
    out.collect("<" + curTs + "> " + keyName + ":" + "温度值连续" + interval/1000 + "秒上升" )
    curTimerTsState.clear()
  }
}
