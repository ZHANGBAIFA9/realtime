package com.myspark.utils

import java.io.InputStream
import java.util.Properties

/**
  * @Version 1.0
  * @Author ZHANGBAIFA
  * @Date 2021/3/25 14:45
  * @Description:
  */
object PropertiesInit {
  // 加载配置文件
  def loadProperties(): Properties = {
    val prop:Properties = new Properties()
    try {
      prop.load(this.getClass.getClassLoader.getResourceAsStream("application.properties"))
    }catch {
      case exception:Exception =>
        print("file not found")
    }
    prop
  }

  //获取文件内配置内容
  def getFileProperties(fileName:String,propertyKey:String):String={
    val result: InputStream = this.getClass.getClassLoader.getResourceAsStream(fileName)
    val prop:Properties = new Properties()
    prop.load(result)
    prop.getProperty(propertyKey)
  }

}
