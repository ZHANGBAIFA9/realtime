package com.myspark.sql

import org.apache.spark.sql.SparkSession

/**
  * @Version 1.0
  * @Author ZHANGBAIFA
  * @Date 2021/3/26 15:58
  * @Description:
  */
object TestHQL {
  def main(args: Array[String]): Unit = {
    System.setProperty( "hadoop.home.dir", "D:\\Hadoop\\hadoop-2.7.3" )
    val spark = SparkSession
      .builder()
      .appName("sparkSQL")
      .master("local[*]")
//      .config("spark.sql.warehouse.dir","hdfs://node1:8020/user/hive_ha/warehouse")
//      .config("hive.metastore.uris","thrift://192.168.76.103:9083")
      .enableHiveSupport()
      .getOrCreate()
    spark.sql("show databases").show()

  }

}
