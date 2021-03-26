package com.myspark.core.wc

import org.apache.spark.rdd.{HadoopRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}
/**
  * @Version 1.0
  * @Author ZHANGBAIFA
  * @Date 2021/3/26 9:26
  * @Description:
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    //1、读取文件
    val word1s:RDD[String] = sc.textFile("E:\\tmp\\input\\word1.txt")
    val word2s:RDD[String] = sc.textFile("E:\\tmp\\input\\word2.txt")
    //切分
    val word1:RDD[String] = word1s.flatMap(_.split(" "))
    val word2:RDD[String] = word2s.flatMap(_.split(" "))
    //标1成对
    val word1Group:RDD[(String,Int)] = word1.map((_,1))
    val word2Group:RDD[(String,Int)] = word2.map((_,1))
    //聚合
    var words:RDD[(String,Int)] = word1Group.union(word2Group)
    //计算
    words.reduceByKey(_+_).collect().foreach(print)
    sc.stop()
  }
}
