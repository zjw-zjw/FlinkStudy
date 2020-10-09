package com.zjw.flink1205.apitest

import com.zjw.flnk1205.function.TempChangeWarning
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state.{ReducingState, ReducingStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object StateTest {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)   // 设置时间语义

    // 配置状态后端
//    env.setStateBackend( new MemoryStateBackend() )
//    env.setStateBackend( new FsStateBackend("") )
//    env.setStateBackend( new RocksDBStateBackend("", true) )


    val inputStream: DataStream[String] = env.socketTextStream("node01", 7777)


    val dataStream = inputStream
      .map( data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      } )
      .keyBy("id")
//      .process(new MyProcessor)
//      .map( new TempChangeWarning(10.0) )
      .flatMapWithState[(String, Double, Double), Double]( {
        case (inputData: SensorReading, None) => (List.empty, Some(inputData.temperature))
        case (inputData: SensorReading, lastTemp: Some[Double]) => {
          val diff = (inputData.temperature - lastTemp.get).abs
          if (diff > 10.0) {
            (List((inputData.id, lastTemp.get, inputData.temperature)), Some(inputData.temperature))
          } else {
            (List.empty, Some(inputData.temperature))
          }
        }
      })

    // 执行
    env.execute("state test job")
  }
}


class MyProcessor extends KeyedProcessFunction[Tuple, SensorReading, Int] {
//  lazy val myState: ValueState[Int] = getRuntimeContext.getState(new ValueStateDescriptor[Int]("my-state", classOf[Int]))

  // 这里等价上面的lazy定义
  var myState: ValueState[Int] = _
  override def open(parameters: Configuration): Unit = {
    myState = getRuntimeContext.getState( new ValueStateDescriptor[Int]("my-state", classOf[Int]))
  }

  lazy val myReducingState: ReducingState[SensorReading] = getRuntimeContext.getReducingState( new ReducingStateDescriptor[SensorReading](
    "my-reducingstate",
    new ReduceFunction[SensorReading] {
      override def reduce(value1: SensorReading, value2: SensorReading): SensorReading = {
        SensorReading(value1.id, value1.timestamp.max(value2.timestamp), value1.temperature.min(value2.temperature))
      }
    },
    classOf[SensorReading]
  ))

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[Tuple, SensorReading, Int]#Context, out: Collector[Int]): Unit = {
//    myReducingState.add(value)
    val keyStr: String = ctx.getCurrentKey.asInstanceOf[Tuple1[String]].f0
  }
}

