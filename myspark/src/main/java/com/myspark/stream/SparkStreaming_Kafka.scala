package com.myspark.stream

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
/**
  * @Version 1.0
  * @Author ZHANGBAIFA
  * @Date 2021/4/6 23:31
  * @Description:
  *               无状态操作
  */
object SparkStreaming_Kafka {
  def main(args: Array[String]): Unit = {
    //1、创建sparkconf
    val conf:SparkConf = new SparkConf().setAppName("SparkStreaming_Kafka").setMaster("local[*]")
    //2、创建StreamingContext
    val ssc:StreamingContext = new StreamingContext(conf,Seconds(3))

    //3、定义kafka参数
    val kafkaPara:Map[String,Object] = Map[String,Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "node2:9092,node3:9092,node4:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "t1",
//      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false", // 关闭自动提交偏移量
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )
    //4、读取kafka数据创建DStream
    val kafkaDStream:InputDStream[ConsumerRecord[String,String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("t1"), kafkaPara)
    )
    //每条记录的value值
    val valueDStream:DStream[String] = kafkaDStream.map(record => record.value())
    valueDStream.print()
    //启动
    ssc.start()
    ssc.awaitTermination()
  }
}
