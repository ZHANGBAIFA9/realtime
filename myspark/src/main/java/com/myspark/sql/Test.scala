package com.myspark.sql

import java.sql.Timestamp

import com.myspark.utils.DXUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @Version 1.0
  * @Author ZHANGBAIFA
  * @Date 2021/3/25 14:28
  * @Description:
  *              scala2.12与2.11：2.11 DataFrame 直接调foreach会报错
  */
object Test {
  def main(args: Array[String]): Unit = {
    System.setProperty( "hadoop.home.dir", "D:\\Hadoop\\hadoop-2.7.3" )
        Logger.getLogger( "org" ).setLevel( Level.ERROR )
        val spark: SparkSession = SparkSession.builder().appName( getClass.getSimpleName ).master( "local[*]" ).getOrCreate()
        val sc: SparkContext = spark.sparkContext
        import spark.implicits._

        val df:DataFrame = DXUtils.conMysql("cs",spark)
        val rows = df.rdd.collect()
        rows.foreach(println)


//    val spark = SparkSession.builder().appName("sparkSQL").master("local[*]").getOrCreate()
//    val url = "jdbc:mysql://127.0.0.1:3306/ceshi?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&useSSL=false"
//    val table = "cs"
//    val prop = new Properties()
//    prop.put("driver", "com.mysql.cj.jdbc.Driver")
//    prop.put("user", "root")
//    prop.put("password", "root")
//    val df1 = spark.read.jdbc(url , table , prop)
//    df1.show(1000,false)
//    spark.sql("show databases").show()
  }

}
