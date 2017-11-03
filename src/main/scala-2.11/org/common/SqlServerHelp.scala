package org.common

import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet

import java.sql.Statement

/**
  * Created by lta on 2017/2/24.
  */
object SqlServerHelp {


  private var conn: Connection = null
  private val driver = "com.mysql.jdbc.Driver"
  //private val driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
  //private val url = "jdbc:sqlserver://192.168.10.249:1433;databaseName=18phone;user=sa;password=pk12580"
  //url="jdbc:mysql://localhost:3306/mysql";
  private val url = "jdbc:mysql://192.168.10.86:3306/beyebe_bigdata"
  private var statement: Statement = null;

  private  val sqlname="root"
  private  val sqlpw="123456"


  def getConnection() {
    try {
      Class.forName(driver)
      if(conn==null || conn.isClosed) conn = DriverManager.getConnection(url, sqlname, sqlpw)
      initStatement()
    } catch {
      case t: Exception =>
        t.printStackTrace()
        throw t
    }
  }


  private def initStatement(): Unit = {
    statement = conn.createStatement()
  }


  ///执行查询语句
  def select(strSelect: String): ResultSet = {
    val resultSet: ResultSet = statement.executeQuery(strSelect)
    resultSet
  }

  def insertUpdateDelete(strSQL: String): Int = {
    return statement.executeUpdate(strSQL)
  }

  def close = if (conn != null && !conn.isClosed()) {
    conn.close()
    conn = null
  }


}
