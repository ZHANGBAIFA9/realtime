package com.myflink.test

import org.apache.flink.streaming.api.scala._

/**
  * @Version 1.0
  * @Author ZHANGBAIFA
  * @Date 2021/3/8 22:38
  * @Description:
  *              flink 流处理测试
  *              1、先起netcat ---> nc -l -p 7777
  *              2、启动程序测试
  */
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    //创建流处理的执行环境
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //接收一个socket文本流
    val inputDataStream:DataStream[String] = env.socketTextStream("127.0.0.1",7777)

    //进行转换处理统计
    val resultDataStream:DataStream[(String,Int)] = inputDataStream
      .flatMap(_.split(" ")) //压扁切割
      .filter(_.nonEmpty) //过滤空值
      .map((_, 1))  //标1 成对
      .keyBy(0) // 按key分组
      .sum(1) //求和

    //打印输出
      resultDataStream.print()
    //启动任务执行,给定作业名
    env.execute("StreamWordCount")

  }
}
