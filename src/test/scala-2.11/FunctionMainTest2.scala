import java.io.{IOException, PrintWriter, StringWriter}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, UUID}

import org.apache.log4j.Logger
import org.common.{SqlServerHelp, UpdateStatus}
import org.tianhong.SparkService2

/**
  * Created by lta on 2017/3/2.
  */

import org.apache.spark.sql._

object FunctionMainTest2 {
  val logger = Logger.getLogger(FunctionMainTest2.getClass)

  def main(args: Array[String]): Unit = {
    var sparkID = UUID.randomUUID().toString
    val build = SparkSession.builder().appName("TianHongCalc-" + sparkID) // .master("local")
    if (!(System.getProperty("os.name").toLowerCase.contains("linux"))) {
      build.master("local")
    }
    val spark: SparkSession = build.getOrCreate()
    var dayNumber = 15
    if (args.length > 0) {
      dayNumber = args(0).toInt
    }
    insertStatusInfo(sparkID)
    //任务信息是否存在
    SqlServerHelp.getConnection()
    val sqlSelect =s"""select DISTINCT platformID  from sparkSchedule where sparkID='${sparkID}' and  statusValue =0 """
    val resultSet = SqlServerHelp.select(sqlSelect)
    var map = Map[Int, String]()
    while (resultSet.next()) {
      val platform = resultSet.getInt("platformID")
      if (platform == 1) {
        map = map.+(1 -> "TM")
      }
//      if (platform == 2) {
//        map = map.+(2 -> "JD")
//      }
//      if (platform == 3) {
//        map = map.+(3 -> "FS")
//      }
    }
    SqlServerHelp.close
    if (map.size < 1) {
      println("----------------------------------------------------------")
      println("----------------------------------------------------------")
      println("没有任务队列")
      println("----------------------------------------------------------")
      println("----------------------------------------------------------")
      spark.stop()
      return
    }
    //  val    sparkID="1231313"

    //    val map = Map[Int, String](
    //      1 -> "TM"
    //      // 2->"JD",
    //      // 3->"FS"
    //    )

    for (elem <- map) {
      logger.info(s"sparkID:${sparkID},platformID:${elem._1}正在开始执行")
      run(spark, sparkID, elem._1, elem._2, dayNumber)
      logger.info(s"sparkID:${sparkID},platformID:${elem._1}执行结束")
    }
    spark.stop()

  }

  def run(spark: SparkSession, sparkID: String, platformID: Int, tablePref: String, dayNumber: Int): Unit = {
    println("执行中")
    /*
            * 0 提交任务
            * 1 任务执行完毕
            * 2 执行中
            * 3 任务失败
            * */
    UpdateStatus.updateStatus(sparkID, platformID, 2, "执行中")
    try {
      SparkService2.run(spark, platformID, tablePref, sparkID, dayNumber)
      //
    } catch {
      case e: Exception => {
        println(e.getMessage)
        var sw: StringWriter = null
        var pw: PrintWriter = null
        var message: String = null
        try {
          sw = new StringWriter()
          pw = new PrintWriter(sw)
          //将出错的栈信息输出到printWriter中
          e.printStackTrace(pw);
          pw.flush();
          sw.flush();
        } finally {
          if (sw != null) {
            try {
              sw.close();
            } catch {
              case e1: IOException => e1.printStackTrace();
            }
          }
          if (pw != null) {
            pw.close();
          }
        }
        message = sw.toString
        UpdateStatus.updateStatus(sparkID, platformID, 3, "任务失败", e.getMessage + "\r\n" + "栈信息：" + message)
        logger.error("sparkID:" + sparkID + "\t" + "platformID:" + platformID + "执行失败:" + e.getMessage + "\r\n" + "栈信息：" + message)
      }
    }
  }


  def runOrExit(): Unit = {

    val sql = "select max(endTime) as endTime from sparkSchedule where statusValue=1"
    val query = SqlServerHelp.select(sql)
    var endTime = ""
    while (query.next()) {
      endTime = query.getString("endTime")
    }
    if (endTime != null) {
      val calendar = Calendar.getInstance();
      calendar.setTime(new Date());
      calendar.add(Calendar.HOUR, -1);
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      val nowDesc = dateFormat.format(calendar.getTime());
      if (!(nowDesc.compareTo(endTime) == -1)) {

      }
    } else {

    }
  }


  def insertStatusInfo(sparkID: String): Unit = {
    var platformID = 1
    SqlServerHelp.getConnection()
    for (i <- 1 to (3)) {
      platformID = i
      val sql =
        s" INSERT into sparkSchedule (platformID,sparkID,startTime,statusName,statusValue) VALUES (${platformID},'${sparkID}',NOW(),'已提交任务',0) "
      SqlServerHelp.insertUpdateDelete(sql)
      logger.info(sql)
    }
    SqlServerHelp.close
  }


  //返回要执行的sparkID 与平台的ID
  def submit(): Map[Int, String] = {
    val sqlStr = "select DISTINCT platformID, sparkID from sparkSchedule where statusValue=0 ORDER BY startTime DESC "
    val result = SqlServerHelp.select(sqlStr)
    //
    var mapTuple = Map[Int, String]()
    //[sparkID,platformID]
    var mapDistinct = Map[Int, String]()
    while (result.next()) {
      val platformID = result.getInt("platformID")
      val sparkID = result.getString("sparkID")
      if (mapTuple.keySet != null && mapTuple.keySet.exists(r => r == platformID)) {
        val sparkid = mapTuple.get(platformID).get
        mapDistinct = mapDistinct.+(platformID -> sparkid)
      } else {
        mapTuple = mapTuple.+(platformID -> sparkID)
      }
    }
    SqlServerHelp.getConnection()
    for (item <- mapDistinct) {
      val updateSql =s"""UPDATE sparkSchedule set statusValue=4,statusName='同一时间段重复提交任务PASS' where sparkID='${item._2}' and platformID=${item._1}"""
      SqlServerHelp.insertUpdateDelete(updateSql)
    }
    SqlServerHelp.close
    mapTuple
  }


  ///判断平台要执行的信息是否在关系数据库表中存在信息，如果没有或不完整则推出当前的任务信息
  def runOrExit2(sparkID: String, arrayPlatformID: Array[Int]): Unit = {
    var list: List[Int] = List()
    for (elem <- arrayPlatformID) {
      if (elem == 1) {
        list = 1 :: list
      }
      if (elem == 2) {
        list = 2 :: list
      }
      if (elem == 3) {
        list = 4 :: list
      }
    }
    SqlServerHelp.getConnection()
    val sum = list.sum
    val selectSql =s""" select platformID as platformID from sparkSchedule where sparkID='${sparkID}'  """
    val result = SqlServerHelp.select(selectSql)
    var resultsum = 0
    while (result.next()) {
      val value = result.getInt("platformID")
      if (value != 1 && value != 2 && value != 3) {
        println(s"关系数据库中包含错误的平台ID")
        System.exit(0)
      }
      val s = if (value == 3) 4 else value
      resultsum = resultsum + s
    }
    if (resultsum < sum) {
      var err = " "
      for (e <- arrayPlatformID) {
        err = err + "--" + e
      }
      println(s"要执行的平台id的为${err}的信息未在sparkSchedule任务表中包含,当前的sparkID：${sparkID}")
      System.exit(0)
    }
  }


}

