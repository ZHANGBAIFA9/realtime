package com.myspark.utils

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, DataFrameReader, SaveMode, SparkSession}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * @Version 1.0
  * @Author ZHANGBAIFA
  * @Date 2021/3/25 16:20
  * @Description:
  *              读写utils
  */
object DXUtils {
  private val conf: Properties = PropertiesInit.loadProperties()
  val writeMode = conf.getProperty("jdbc.write.mode")
  //设置rds mysql读参数
  val urlIn = conf.getProperty("jdbc.read.url")
  val propIn = new Properties()
  propIn.put("user", conf.getProperty("jdbc.read.user"))
  propIn.put("password", conf.getProperty("jdbc.read.password"))
  propIn.put("driver", conf.getProperty("jdbc.mysql.driver"))
  //设置rds mysql写参数
  val urlOut = conf.getProperty("jdbc.write.url")
  val propOut = new Properties()
  propOut.put("user", conf.getProperty("jdbc.write.user"))
  propOut.put("password", conf.getProperty("jdbc.write.password"))
  propOut.put("driver", conf.getProperty("jdbc.mysql.driver"))
  propOut.put("batchsize", conf.getProperty("jdbc.write.batchsize"))

  /**
    * **************************************************************************
    * 查询数据库
    * **************************************************************************
    */
  //spark读mysql，普通读取
  def conMysql(dbtable: String, spark: SparkSession): DataFrame = {
    val dataFrame = spark.read.jdbc(urlIn, dbtable, propIn)
    dataFrame
  }

  //传入数据范围读取
  def conMysql(dbtable: String, spark: SparkSession, predicates: Array[String]): DataFrame = {
    predicates.foreach(println(_))
    val dataFrame = spark.read.jdbc(urlIn, dbtable, predicates, propIn)
    dataFrame
  }

  //设置字段读取范围并行读取
  def conMysql(dbtable: String, spark: SparkSession, partitionColumn: String, lowerBound: Long, upperBound: Long, numPartitions: Int): DataFrame = {
    val dataFrame = spark.read.jdbc(urlIn, dbtable, partitionColumn, lowerBound, upperBound, numPartitions, propIn)
    dataFrame
  }

  //使用完整的子查询语句从mysql数据库中查询数据
  def conMysql1QueryStadement(dbname: String, queryStadement: String, spark: SparkSession): DataFrame = {
    if (dbname.startsWith("fmid_source")) {
      DXUtils.readMysqlConf(spark).option("query", s"${queryStadement} ").load()
    } else {
      DXUtils.writeMysqlConf(spark).option("query", s"${queryStadement} ").load()
    }
  }

  //手动设置字段的取值范围读取数据
  def conMysql2Step(dbtable: String, col: String, minMaxStep: (Int, Int), spark: SparkSession): DataFrame = {
    conMysql(dbtable, spark, predicates4(col, minMaxStep))
  }

  def conMysql2StepBig(dbtable: String, col: String, minMaxStep: (Long, Long), spark: SparkSession): DataFrame = {
    conMysql(dbtable, spark, predicates4Big(col, minMaxStep))
  }

  def conMysqlUp(dbtable: String, col: String, table1as: String, spark: SparkSession): Unit = {
    DXUtils.readMysqlConf(spark).option("query", s"select * from ${dbtable} where data_status='U'").load().createOrReplaceTempView(table1as)
  }

  //查询数据库读取N条的符合要求的数据(在副表中取)
  def conMysqlChangeDF(table1as: String, table1conkey: String, dbtable2: String, table2conkey: String, table2as: String,
                       spark: SparkSession) = {
    val longs: Array[String] = spark.sql(s"select ${table1conkey} from ${table1as} ").rdd.map {
      x => {
        if (x.isNullAt(0)) {
          null
        } else {
          x.get(0).toString
        }
      }
    }.collect()
    //      val str = longs.mkString(",")
    var str = ""
    if (dbtable2 == "fmid_source.party_milkvip") {
      val listBuffer = new ListBuffer[String]
      for (i <- 0 to longs.length - 1) {
        val str = "milkman-" + longs(i)
        listBuffer += str
      }
      str = listBuffer.mkString("\"", "\",\"", "\"")
    } else {
      str = longs.mkString("\"", "\",\"", "\"")
    }
    DXUtils.conMysqlQuery(dbtable2, s"${table2conkey} in ($str)", spark).createOrReplaceTempView(table2as)
  }

  //查询一条固定的sql语句
  def conMysqlQuery(dbtable: String, colname: String, condition: String, spark: SparkSession): DataFrame = {
    DXUtils.commenMysqlConf(spark).option("query", s"select $colname from ${dbtable} where $condition ").load()
  }

  //查询一条固定的sql语句
  def conMysqlQuery(dbtable: String, condition: String, spark: SparkSession): DataFrame = {
    DXUtils.readMysqlConf(spark).option("query", s"select * from ${dbtable} where $condition ").load()
  }

  /**
    * ***************************************************************
    * 全量计算
    * ***************************************************************
    */
  //拉取incr_pk设定值之前的数据，用于全量计算
  def getDataBeforeIncrpk(dbtable: String, col: String, sc: SparkContext, spark: SparkSession): DataFrame = {
    val db_name = dbtable.split("\\.")(0)
    val tbl_name = dbtable.split("\\.")(1)
    val minmaxpkFrame = DXUtils.readMysqlConf(spark).option("query",
      s"""
         |select min_incr_pk,max_incr_pk from fmid_source.min_max_incr_pk_table
         |where tbl_name ='${tbl_name}'
         |""".stripMargin).load()
    val minMaxStep: (Long, Long) = minmaxpkFrame.rdd.map(step => (step.getLong(0), step.getLong(1))).take(1)(0)
    conMysql(dbtable, spark, predicates4Big(col, minMaxStep))
  }

  def setMinMaxIncrpkOrId2PkTbl(dbtable: String, col: String, sc: SparkContext, spark: SparkSession): Unit = {
    import spark.implicits._
    val db_name = dbtable.split("\\.")(0)
    val tbl_name = dbtable.split("\\.")(1)
    val min_max_value: (Long, Long) = DXUtils.readMysqlConf(spark).option("query",
      s"""
         |select min(${col}),max(${col}) from ${dbtable}
         |""".stripMargin).load().rdd.map(step => (step.get(0).asInstanceOf[Long], step.get(1).asInstanceOf[Long])).take(1)(0)
    val dataFrame = sc.parallelize(Seq((db_name, tbl_name, min_max_value._1, min_max_value._2))).toDF("db_name", "tbl_name", "min_incr_pk", "max_incr_pk")
    MySQLUtils.insertOrUpdateDFtoDB("fmid_source.min_max_incr_pk_table", dataFrame, Array("db_name", "tbl_name", "min_incr_pk", "max_incr_pk"))

  }

  /**
    * *******************************************************************
    * 增量计算
    * *******************************************************************
    */
  def getDataAfterIncrpk(dbtable: String, col: String, sc: SparkContext, spark: SparkSession): DataFrame = {
    val db_name = dbtable.split("\\.")(0)
    val tbl_name = dbtable.split("\\.")(1)
    val minmaxpkFrame = DXUtils.readMysqlConf(spark).option("query",
      s"""
         |select max_incr_pk from fmid_source.min_max_incr_pk_table
         |where tbl_name ='${tbl_name}'
         |""".stripMargin).load()
    val minStep = minmaxpkFrame.rdd.map(step => step.getLong(0)).take(1)(0) + 1
    val maxStep = DXUtils.readMysqlConf(spark).option("query", s"select max($col) from ${dbtable}")
      .load().rdd.map(step => step.get(0).toString.toLong).take(1)(0)
    conMysql(dbtable, spark, predicates4Big(col, (minStep, maxStep)))
  }

  def getPrimaryKeysFromIncrDown( sqlStr:String, sc: SparkContext, spark: SparkSession): Unit = {


  }

  /**
    * *****************************************************************
    * 写数据库
    * *****************************************************************
    */

  // spark写mysql数据库，通用方法
  def writeMysql(dbtable: String, col: String, dataFrame: DataFrame, sc: SparkContext, spark: SparkSession): Unit = {
    dataFrame.write.mode(saveMode(writeMode)).jdbc(urlOut, dbtable, propOut)
    setMinMaxIncrpkOrId2PkTbl(dbtable, col, sc, spark)
  }
  def writeMysql(dbtable: String,  dataFrame: DataFrame): Unit = {
    dataFrame.write.mode(saveMode(writeMode)).jdbc(urlOut, dbtable, propOut)
  }
  // 选择写mysql模式，默认append
  def saveMode(writeMode: String): SaveMode = {
    var mode: SaveMode = SaveMode.Append
    if (writeMode.equals("overwrite")) {
      mode = SaveMode.Overwrite
    } else {
      println("please add SaveMode")
    }
    mode
  }

  /**
    * **********************************************************************
    * 数据精准并行读取
    * **********************************************************************
    */

  //精准设置spark读取mysql的并行度，保障数据不丢失。用于设置并行度的列为int类型
  def predicates4(col: String, minMaxStep: (Int, Int)): Array[String] = {
    val minStep = minMaxStep._1
    val maxStep = minMaxStep._2
    val counts = maxStep - minStep
    val step = conf.getProperty("spark.read.mysql.step").toInt
    val arrayBuffer = ArrayBuffer[(Int, Int)]()
    if (maxStep - minStep < 2 * step) {
      arrayBuffer.append((minStep, maxStep))
      arrayBuffer.toArray.map {
        case (s, e) => {
          s"$col >= $s " + s"AND $col <= $e"
        }
      }
    } else {
      if (counts % step == 0) {
        val part = (counts / step).intValue()
        for (i <- 0 to part - 1) {
          val start = minStep + i * step
          val end = minStep + (i + 1) * step
          arrayBuffer.append((start, end))
        }
        arrayBuffer.append((maxStep, maxStep + 1))
      } else {
        val part = (counts / step).intValue()
        for (i <- 0 to part - 1) {
          val start = minStep + i * step
          val end = minStep + (i + 1) * step
          arrayBuffer.append((start, end))
        }
        arrayBuffer.append((minStep + part * step, maxStep + 1))
      }
      arrayBuffer.toArray.map {
        case (s, e) => {
          s"$col >= $s " + s"AND $col < $e"
        }
      }

    }

  }

  //精准设置spark读取mysql的并行度，保障数据不丢失。用于设置并行度的列为BigInt类型
  def predicates4Big(col: String, minMaxStep: (Long, Long)): Array[String] = {
    val minStep = minMaxStep._1
    val maxStep = minMaxStep._2
    val counts = maxStep - minStep
    val step = conf.getProperty("spark.read.mysql.step").toInt
    val arrayBuffer = ArrayBuffer[(Long, Long)]()
    if (maxStep - minStep < 2 * step) {
      arrayBuffer.append((minStep, maxStep))
      arrayBuffer.toArray.map {
        case (s, e) => {
          s"$col >= $s " + s"AND $col <= $e"
        }
      }
    } else {
      if (counts % step == 0) {
        val part = (counts / step).intValue()
        for (i <- 0 to part - 1) {
          val start = minStep + i * step
          val end = minStep + (i + 1) * step
          arrayBuffer.append((start, end))
        }
        arrayBuffer.append((maxStep, maxStep + 1))
      } else {
        val part = (counts / step).intValue()
        for (i <- 0 to part - 1) {
          val start = minStep + i * step
          val end = minStep + (i + 1) * step
          arrayBuffer.append((start, end))
        }
        arrayBuffer.append((minStep + part * step, maxStep + 1))
      }
      arrayBuffer.toArray.map {
        case (s, e) => {
          s"$col >= $s " + s"AND $col < $e"
        }
      }
    }
  }

  /**
    * ************************************************************************
    * 数据库连接配置
    * ***********************************************************************
    */
  //读mysql连接reader
  def readMysqlConf(spark: SparkSession): DataFrameReader = {
    val reader: DataFrameReader = spark.read.format("jdbc").option("url", urlIn).option("user", conf.getProperty("jdbc.read.user"))
      .option("password", conf.getProperty("jdbc.read.password"))
    reader
  }

  //写mysql连接reader
  def writeMysqlConf(spark: SparkSession): DataFrameReader = {
    val reader: DataFrameReader = spark.read.format("jdbc").option("url", urlIn).option("user", conf.getProperty("jdbc.read.user"))
      .option("password", conf.getProperty("jdbc.read.password"))
    reader
  }

  //mysql连接公共reader
  def commenMysqlConf(spark: SparkSession): DataFrameReader = {
    val reader: DataFrameReader = spark.read.format("jdbc").option("url", conf.getProperty("mysql.jdbc.url")).option("user", conf.getProperty("mysql.jdbc.username"))
      .option("password", conf.getProperty("mysql.jdbc.password"))
    reader
  }

  /**
    * ***************************************************************************************
    * 暂时弃用
    * ***************************************************************************************
    */

  //全量计算：根据col字段读取该字段的最大值和最小值，自动设置并行度，并将当前读取到的最大值记录到数据库，用于并行的col值有Int,BigInt三种。
  def conMysql2Step(dbtable: String, col: String, sc: SparkContext, spark: SparkSession): DataFrame = {
    if (dbtable.startsWith("ceshi")) {
      val dataFrame = DXUtils.readMysqlConf(spark).option("query", s"select min($col),max($col) from ${dbtable}").load()
      val minMaxStep: (Int, Int) = dataFrame.rdd.map(step => (step.getInt(0), step.getInt(1))).take(1)(0)
      conMysql(dbtable, spark, predicates4(col, minMaxStep))
    }
    else {
      val dataFrame = DXUtils.writeMysqlConf(spark).option("query", s"select min($col),max($col) from $dbtable ").load()
      val minMaxStep: (Int, Int) = dataFrame.rdd.map(step => (step.getInt(0), step.getInt(1))).take(1)(0)
      conMysql(dbtable, spark, predicates4(col, minMaxStep))
    }
  }

  def conMysql2StepBig(dbtable: String, col: String, sc: SparkContext, spark: SparkSession): DataFrame = {
    if (dbtable.startsWith("fmid_source")) {
      val dataFrame = DXUtils.readMysqlConf(spark).option("query", s"select min($col),max($col) from ${dbtable}").load()
      val minMaxStep: (Long, Long) = dataFrame.rdd.map(step => (step.getLong(0), step.getLong(1))).take(1)(0)
      conMysql(dbtable, spark, predicates4Big(col, minMaxStep))
    }
    else {
      val dataFrame = DXUtils.writeMysqlConf(spark).option("query", s"select min($col),max($col) from $dbtable ").load()
      val minMaxStep = dataFrame.rdd.map(step => (step.getLong(0), step.getLong(1))).take(1)(0)
      conMysql(dbtable, spark, predicates4Big(col, minMaxStep))
    }
  }

  // 通过中间库设置的自增列设置spark读取mysql数据的并行度。弃用
  def predicates3(colname: String, count: Int, part: Int): Array[String] = {
    val arrayBuffer = ArrayBuffer[(Int, Int)]()
    val step = (count / part).intValue()
    for (i <- 0 to part - 1) {
      val start = ((step + 1) * i)
      val end = ((step + 1) * (i + 1))
      arrayBuffer.append((start, end))
    }
    arrayBuffer.toArray.map {
      case (s, e) => {
        s"$colname >= $s " + s"AND $colname < $e"
      }
    }
  }

  // 数据并行读取度设置方法，设置离散的数值作为数据分割点，形成数组。弃用
  def predicates2(colname: String, array: Array[Int]): Array[String] = {
    val arrayBuffer = ArrayBuffer[(Int, Int)]()
    for (i <- 0 to array.length - 2) {
      arrayBuffer.append((array(i), array(i + 1)))
    }
    arrayBuffer.toArray.map {
      case (s, e) => {
        s"$colname >= $s " + s"AND $colname < $e"
      }
    }
  }

  //  数据并行读取度设置方法，设置离散的时间点作为数据分割点，形成数组。弃用
  def predicates(timestamp: String, array: Array[String]): Array[String] = {
    val arrayBuffer = ArrayBuffer[(String, String)]()
    for (i <- 0 to array.length - 2) {
      arrayBuffer.append((array(i), array(i + 1)))
    }
    arrayBuffer.toArray.map {
      case (s, e) => {
        s"date($timestamp) >= date('$s') " + s"AND date($timestamp) < date('$e')"
      }
    }
  }

  //增量读取mysql中bigdecimal中的数据，暂时弃用。
  def conMysqlIncrBigDe(dbtable: String, col: String, table1as: String, spark: SparkSession): Unit = {
    DXUtils.conMysql("fmid_source.max_incr_pk_table", spark).createOrReplaceTempView("mipt")
    val minStep = spark.sql(s"select incr_pk from(select *,row_number() over(partition by tbl_name order by data_createtime desc) as rank from mipt) t where t.rank=1 and t.tbl_name='${dbtable.split("\\.")(1)}'")
      .rdd.map(step => step.getDecimal(0)).take(1)(0)
    val maxStep = DXUtils.readMysqlConf(spark).option("query", s"select max($col) from ${dbtable}").load()
      .rdd.map(step => step.getInt(0)).take(1)(0).asInstanceOf[java.math.BigDecimal]
    val minMaxStep = (minStep, maxStep)
    //    conMysql(dbtable, spark, predicates4BigDeci(col, minMaxStep)).createOrReplaceTempView(table1as)
  }

  def getAllTempView(sc: SparkContext, spark: SparkSession): Unit = {
    val tableTuple: Array[(String, String, Long, Long)] = DXUtils.conMysql("fmid_source.min_max_incr_pk_table", spark).rdd.map(msg => (msg.getString(0), msg.getString(1), msg.getLong(2), msg.getLong(3)))
      .collect()
    for (i <- 0 to tableTuple.length - 1) {
      val dbname = tableTuple(i)._1
      val tabname = tableTuple(i)._2
      val minincrpk = tableTuple(i)._3
      val maxincrpk = tableTuple(i)._4
      tabname match {
        case "sys_user" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("id", (minincrpk, maxincrpk))).createOrReplaceTempView("sua")
        case "platform_user" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("id", (minincrpk, maxincrpk))).createOrReplaceTempView("pua")
        case "user_login_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("ula")
        case "party_role_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("proa")
        case "base_milkman_mzcrm" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("bma")
        case "sys_wechat" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("swa")
        case "person_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("pa")
        case "user_login_union_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("ulua")
        case "user_head_img_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("uhia")
        case "party_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("para")
        case "milkcard_user_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("mua")
        case "postal_address_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("paa")
        case "party_profile_default_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("ppda")
        case "party_contact_mech_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("pcma")
        case "geo_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("geoa")
        case "party_contact_mech_purpose_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("pcmpa")
        case "super_mem_order_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("smoa")
        case "super_mem_info_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("smia")
        case "user_grade_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("uga")
        case "product_promo_userlogin_union_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("ppuua")
        case "product_store_promo_appl_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("pspaa")
        case "product_promo_code_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("ppca")
        case "user_sign_grown_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("usga")
        case "user_order_grown_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("uoga")
        case "points_total_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("pta")
        case "points_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("poa")
        case "order_item_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("oia")
        case "ord_web_daily_mzcrm" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("owda")
        case "group_info_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("gia")
        case "group_customer_info_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("gcia")
        case "group_adjument_role_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("garoa")
        case "group_adjument_rule_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("garua")
        case "base_dealer_mzcrm" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("bda")
        case "base_dealer_api_mzcrm" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("bdaa")
        case "BASE_DEALER_MACHINE_mzcrm" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("bdma")
        case "BASE_DEALER_MACH_ORG_mzcrm" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("bdmoa")
        case "BASE_ORG_mzcrm" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("boa")
        case "ORD_WEB_BOOK_MZCRM" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("owba")
        case "BASE_CUSTOMER_MZCRM" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("bca")
        case "BASE_DEALER_MACH_ORD_mzcrm" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("bdmorda")


        case _ => println("unknown table,please check:" + dbname + "." + tabname)
      }
    }
  }

  def getIncrTempView(sc: SparkContext, spark: SparkSession): Unit = {
    val tableTuple: Array[(String, String, Long, Long)] = DXUtils.conMysql("fmid_source.min_max_incr_pk_table", spark).rdd.map(msg => (msg.getString(0), msg.getString(1), msg.getLong(2), msg.getLong(3)))
      .collect()
    for (i <- 0 to tableTuple.length - 1) {
      val dbname = tableTuple(i)._1
      val tabname = tableTuple(i)._2
      val minincrpk = tableTuple(i)._4 + 1
      var maxincrpk = 0L
      tabname match {
        case "sys_user" => maxincrpk = DXUtils.readMysqlConf(spark).option("query", s"select max(id) from ${dbname + "." + tabname}").load().rdd.map(step => step.getLong(0)).take(1)(0)
        case "platform_user" => maxincrpk = DXUtils.readMysqlConf(spark).option("query", s"select max(id) from ${dbname + "." + tabname}").load().rdd.map(step => step.getInt(0)).take(1)(0).asInstanceOf[Long]
        case _ => maxincrpk = DXUtils.readMysqlConf(spark).option("query", s"select max(incr_pk) from ${dbname + "." + tabname}").load().rdd.map(step => step.getInt(0)).take(1)(0).asInstanceOf[Long]
      }

      tabname match {
        case "sys_user" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("id", (minincrpk, maxincrpk))).createOrReplaceTempView("sui")
        case "platform_user" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("id", (minincrpk, maxincrpk))).createOrReplaceTempView("pui")
        case "user_login_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("uli")
        case "party_role_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("proi")
        case "base_milkman_mzcrm" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("bmi")
        case "sys_wechat" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("swi")
        case "person_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("pi")
        case "user_login_union_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("ului")
        case "user_head_img_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("uhii")
        case "party_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("pari")
        case "milkcard_user_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("mui")
        case "postal_address_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("pai")
        case "party_profile_default_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("ppdi")
        case "party_contact_mech_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("pcmi")
        case "geo_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("geoi")
        case "party_contact_mech_purpose_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("pcmpi")
        case "super_mem_order_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("smoi")
        case "super_mem_info_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("smii")
        case "user_grade_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("ugi")
        case "product_promo_userlogin_union_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("ppuui")
        case "product_store_promo_appl_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("pspai")
        case "product_promo_code_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("ppci")
        case "user_sign_grown_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("usgi")
        case "user_order_grown_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("uogi")
        case "points_total_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("pti")
        case "points_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("poi")
        case "order_item_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("oii")
        case "ord_web_daily_mzcrm" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("owdi")
        case "group_info_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("gii")
        case "group_customer_info_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("gcii")
        case "group_adjument_role_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("garoi")
        case "group_adjument_rule_milkvip" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("garui")
        case "base_dealer_mzcrm" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("bdi")
        case "base_dealer_api_mzcrm" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("bdai")
        case "BASE_DEALER_MACHINE_mzcrm" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("bdmi")
        case "BASE_DEALER_MACH_ORG_mzcrm" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("bdmoi")
        case "BASE_ORG_mzcrm" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("boi")
        case "ORD_WEB_BOOK_MZCRM" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("owbi")
        case "BASE_CUSTOMER_MZCRM" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("bci")
        case "BASE_DEALER_MACH_ORD_mzcrm" => DXUtils.conMysql(dbname + "." + tabname, spark, predicates4Big("incr_pk", (minincrpk, maxincrpk))).createOrReplaceTempView("bdmordi")


        case _ => println("unknown table,please check:" + dbname + "." + tabname)
      }
    }
  }

  //spark记录全量处理后数据的最后位置，用结束位置+1作为下次增量处理的起始值记录在数据库中。
  def write2Mysql(dbtable: String, maxincrpk: Long, sc: SparkContext, spark: SparkSession): Unit = {
    import spark.implicits._
    val db_name = dbtable.split("\\.")(0)
    val tbl_name = dbtable.split("\\.")(1)
    val date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
    println(Seq((db_name, tbl_name, maxincrpk + 1, date)))
    val value: DataFrame = sc.parallelize(Seq((db_name, tbl_name, maxincrpk + 1, date))).toDF("db_name", "tbl_name", "incr_pk", "data_createtime")
    value.show()
    value.write.mode(saveMode(writeMode)).jdbc(urlOut, "fmid_source.max_incr_pk_table", propOut)
  }

  def conMysqlIncrBigInt(dbtable: String, col: String, table1as: String, spark: SparkSession): Unit = {
    DXUtils.conMysql("fmid_source.max_incr_pk_table", spark).createOrReplaceTempView("mipt")
    val minStep = spark.sql(s"select incr_pk from(select *,row_number() over(partition by tbl_name order by data_createtime desc) as rank from mipt) t where t.rank=1 and t.tbl_name='${dbtable.split("\\.")(1)}'")
      .rdd.map(step => step.getLong(0)).take(1)(0)
    val maxStep = DXUtils.readMysqlConf(spark).option("query", s"select max($col) from ${dbtable}").load()
      .rdd.map(step => step.getLong(0)).take(1)(0)
    val minMaxStep = (minStep, maxStep)
    println(minMaxStep)
    conMysql(dbtable, spark, predicates4Big(col, minMaxStep)).createOrReplaceTempView(table1as)
  }

  //增量新增计算：自动设置字段的取值范围，读取数据用于增量计算
  def conMysqlIncrInt(dbtable: String, col: String, table1as: String, spark: SparkSession): Unit = {
    DXUtils.conMysql("fmid_source.max_incr_pk_table", spark).createOrReplaceTempView("mipt")
    val minStep: Long = spark.sql(s"select incr_pk from(select *,row_number() over(partition by tbl_name order by data_createtime desc) as rank from mipt) t where t.rank=1 and t.tbl_name='${dbtable.split("\\.")(1)}'").rdd.map(step => step.getLong(0)).take(1)(0)
    println(minStep)
    val maxStep = DXUtils.readMysqlConf(spark).option("query", s"select max($col) from ${dbtable}").load().rdd.map(step => step.getInt(0)).take(1)(0).asInstanceOf[Long]
    println(maxStep)
    val minMaxStep = (minStep, maxStep)
    DXUtils.conMysql(dbtable, spark, DXUtils.predicates4Big(col, minMaxStep)).createOrReplaceTempView(table1as)
  }
}
