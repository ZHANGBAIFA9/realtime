package com.myspark.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Version 1.0
  * @Author ZHANGBAIFA
  * @Date 2021/4/7 19:31
  * @Description:
  * 带状态操作
  * 1、先起netcat ---> nc -l -p 9999
  * 2、启动程序测试
  */
object SparkStreaming_state {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkStreaming_kafka_state").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(2))
    //设置检查点存放路径
    ssc.checkpoint("cp")
    //数据处理
    val datas = ssc.socketTextStream("localhost",9999)

    val wordToOne = datas.flatMap(_.split(" ")).map((_,1))

    //无状态进行聚合
    //val wordToCount = wordToOne.reduceByKey(_+_)

    //有状态进行聚合,updateStateByKey根据key的状态进行更新
    //参数中两个值：第一个值表示相同key的value值，第二个值表示缓冲区相同的key的value值
    //有状态操作时需设定检查点路径
    val state = wordToOne.updateStateByKey(
      (seq:Seq[Int] , buff:Option[Int]) => {
        val newCount = buff.getOrElse(0) + seq.sum
        Option(newCount)
      }
    )
    state.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
