package com.zjw.flink1205.wc

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}


object DataSetWcApp {

  def main(args: Array[String]): Unit = {


    // 1 env
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    // 2 source
    val txtDataSet: DataSet[String] = env.readTextFile("D:\\IdeaProjects\\flink1205\\src\\data\\hello.txt")

    // 3 transform
    val aggSet: AggregateDataSet[(String, Int)] = txtDataSet.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)


    // 4 sink
    aggSet.print()
  }

}
