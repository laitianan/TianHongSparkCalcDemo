package org.common

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.log4j.Logger

/**
  * Created by lta on 2017/3/9.
  */
object UpdateStatus {
  val logger = Logger.getLogger(UpdateStatus.getClass)
  def updateStatus(sparkID: String, platformID: Int, statusValue: Int, statusName: String, info: String = ""): Unit = {
    SqlServerHelp.getConnection()
    /*
    * 0 已提交任务
    * 1 任务执行完毕
    * 2 执行中
    * 3 任务失败
    * */
    var updateSql = ""
    var sim = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val dateStr = sim.format(new Date())
    if (info != "") {

      updateSql =s"""update sparkSchedule set statusValue=${statusValue},statusName='${statusName}',endTime='${dateStr}',mark='${info}',mark='${info}'  where platformID=${platformID} and  sparkID='${sparkID}' """
    } else {
      updateSql =s"""update sparkSchedule set statusValue=${statusValue},statusName='${statusName}',endTime='${dateStr}' where platformID=${platformID} and  sparkID='${sparkID}' """
    }

    logger.info("sparkID:" + sparkID + "\t" + "platformID:" + "更新状态:"+statusName+"，sql：" + updateSql)
    SqlServerHelp.insertUpdateDelete(updateSql)
    SqlServerHelp.close
  }


}
