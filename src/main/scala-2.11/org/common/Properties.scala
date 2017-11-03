package org.common

import java.util.Properties

/**
  * Created by lta on 2017/3/3.
  */
object PropertiesHelp {
  val prop = new Properties()
  prop.put("user", "root")
  prop.put("password", "123456")
  prop.put("driver", "com.mysql.jdbc.Driver")
  def getProperties(): Properties = {
    prop
  }

}
