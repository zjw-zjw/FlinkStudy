package com.zjw.flink1205.apitest

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala._

object CheckpointTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)   // 设置时间语义

    // 配置状态后端
    //    env.setStateBackend( new MemoryStateBackend() )
    //    env.setStateBackend( new FsStateBackend("") )
    //    env.setStateBackend( new RocksDBStateBackend("", true) )

    // 开启checkpoint配置，指定触发检查点的间隔时间(毫秒)
    env.enableCheckpointing(1000L)
    // 其他配置
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(30000L)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500L)
    env.getCheckpointConfig.setPreferCheckpointForRecovery(true)
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(3)

    // 重启策略的配置
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000L))
    env.setRestartStrategy(RestartStrategies.failureRateRestart(5, Time.of(5, TimeUnit.MINUTES), Time.of(10, TimeUnit.SECONDS)))


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
