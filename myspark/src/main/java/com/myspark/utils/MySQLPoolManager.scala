package com.myspark.utils

import java.sql.Connection

import com.mchange.v2.c3p0.ComboPooledDataSource

/**
  * @Version 1.0
  * @Author ZHANGBAIFA
  * @Date 2021/3/25 15:02
  * @Description:
  */
object MySQLPoolManager {
  var mysqlManager: MysqlPool = _
  //
  def getMysqlManager: MysqlPool = {
    synchronized {
      if (mysqlManager == null) {
        mysqlManager = new MysqlPool
      }
    }
    mysqlManager
  }

  // 初始化连接池，指定写入参数
  class MysqlPool extends Serializable {
    private val cpds: ComboPooledDataSource = new ComboPooledDataSource( true )
    //获取参数值
    try {
      cpds.setJdbcUrl( PropertiesInit.getFileProperties( "application.properties", "jdbc.write.url" ) )
      cpds.setDriverClass( PropertiesInit.getFileProperties( "application.properties", "jdbc.mysql.driver" ) )
      cpds.setUser( PropertiesInit.getFileProperties( "application.properties", "jdbc.write.user" ) )
      cpds.setPassword( PropertiesInit.getFileProperties( "application.properties", "jdbc.write.password" ) )
      cpds.setMinPoolSize( PropertiesInit.getFileProperties( "application.properties", "mysql.pool.jdbc.minPoolSize" ).toInt )
      cpds.setMaxPoolSize( PropertiesInit.getFileProperties( "application.properties", "mysql.pool.jdbc.maxPoolSize" ).toInt )
      cpds.setAcquireIncrement( PropertiesInit.getFileProperties( "application.properties", "mysql.pool.jdbc.acquireIncrement" ).toInt )
      cpds.setMaxStatements( PropertiesInit.getFileProperties( "application.properties", "mysql.pool.jdbc.maxStatements" ).toInt )
    } catch {
      case e: Exception => e.printStackTrace()
    }
    //获取连接
    def getConnection: Connection = {
      try {
        cpds.getConnection()
      } catch {
        case ex: Exception =>
          ex.printStackTrace()
          null
      }
    }
    //关闭连接
    def close(): Unit = {
      try {
        cpds.close()
      } catch {
        case ex: Exception =>
          ex.printStackTrace()
      }
    }
  }
}
