package com.myflink.test

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala._

/**
  * @Version 1.0
  * @Author ZHANGBAIFA
  * @Date 2021/3/8 18:35
  * @Description:
  *              flink批处理测试
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    // 从文件中读取数据
    val inpath:String = "E:\\workSpace\\realtime\\myflink\\src\\main\\resources\\hello.txt"
    val inputDS:DataSet[String] = env.readTextFile(inpath)
    //对源数据进行操作
    val WordCountDS:AggregateDataSet[(String,Int)] = inputDS
      .flatMap(_.split(" ")) //压扁切分
      .map((_,1)) // 标1成对
      .groupBy(0) //通过key进行分组
      .sum(1) //求和
    //打印输出
    WordCountDS.print()


  }
}
