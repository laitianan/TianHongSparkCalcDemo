package org.tianhong

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._
import org.common.{PropertiesHelp, SqlServerHelp, UpdateStatus}

import scala.collection.mutable

/**
  * Created by lta on 2017/3/3.
  */

/*
* val spark = SparkSession.builder().master("local").appName("HBaseTest").getOrCreate()

* */
object SparkService2 {
  val logger = Logger.getLogger(SparkService2.getClass);
  val service = new TianHongCalc()

  def run(spark: SparkSession, platformID: Int, pref: String, sparkID: String, nDay: Int = 15): Unit = {
    //表名前缀
    val preF: String = pref
    //monthlySales
    import spark.implicits._
    val sc = spark.sparkContext
    //天猫1
    //一号店3
    //京东2
    val tableName: String = preF + "ProductDetail"
    logger.info("获取整个表" + tableName)
    val result = service.getResultProductDetail(sc, platformID, tableName, nDay, sparkID)
    val sourceDF = result.toDF()
//    sourceDF.show()
//    if (true) return

    logger.info("缓存表数据" + tableName)
    sourceDF.createOrReplaceTempView("ProductTable")
    logger.info("注册ProductTable")
    val sqlContext = spark.sqlContext

    // val list = List("洗发")
    val list = List("洗发", "护发", "沐浴露", "牙膏", "护肤品", "日本进口零食")
    // val list = List("牙膏")
    //val list = List("洗发")
    var allRdd: DataFrame = null
    for (item <- list) {
      var sortType = " desc "
      var sortFie = ""
      if (platformID == 1) {
        sortFie = "monthlySales"
      }
      else if ((platformID == 2 || platformID == 3) && !item.equals("日本进口零食")) {
        sortFie = "index"
        sortType = " asc "
      } else {
        sortFie = "commentCount"
      }
      //and isWorldWide !='true'
      var sql =
        s"""select  productID, platformID,productName, productBrand,productSpec,productPrice,commentCount, enshrineCount,weeklySales,monthlySales,salesEvents,category,standardCategory,downloadTime,productHotSellID,productArea, fitPeople,productCode,categoryPath,index,sparkID  from ProductTable  where standardCategory  = '${item}'  and productPrice>0 and downloadTime <'3000' and productSpec !='此商品已下架' and (isWorldWide is null or  isWorldWide != 'true') and (groupBuying is null or  groupBuying != 'true')   order by ${sortFie} ${sortType} limit 100
        """.stripMargin
      logger.info("sparkSql查询数据语句" + sql)
      val DF = sqlContext.sql(sql)
      logger.info("sparkSql查询数据语句执行")
      println(item + "-----------------------------------")
      if (allRdd == null) {
        allRdd = DF
      } else {
        allRdd = allRdd.union(DF)
        logger.info("整个表执行Union：" + item)
      }
    }
    logger.info("导入top100 商品开始")
    allRdd.persist()
    allRdd.write.mode("append").jdbc("jdbc:mysql://192.168.10.86:3306/beyebe_bigdata", "product", PropertiesHelp.getProperties())
    logger.info("导入top100 商品完毕" + preF)
    val hotSellIDArray = allRdd.map {
      r =>
        (r.getAs[String]("productID"), r.getAs[String]("productHotSellID"))
    }.collect()
    //  hotSellIDArray.foreach(println _)
    val hotSellIDBroadcast = sc.broadcast(hotSellIDArray)
    allRdd.unpersist()
    //------------------------计算爆品开始
    val hotShellResultRdd = service.getResultHotShellResult(sc, preF + "HotShell", sparkID, platformID)
    val hotShellResuls = hotShellResultRdd.map {
      r =>
        if (hotSellIDBroadcast.value.exists(r2 => r2._2 != null && r2._2.equals(r.productHotSellID))) {
          r
        } else {
          null
        }
    }.filter(r => r != null)

    // hotShellResuls.foreach(r => println(r))
    logger.info("导入top100爆品开始" + preF)
    hotShellResuls.toDF().write.mode("append").jdbc("jdbc:mysql://192.168.10.86:3306/beyebe_bigdata", "hotsell", PropertiesHelp.getProperties())
    logger.info("导入top100爆品结束" + preF)
    //------------------------计算爆品结束
    /////////////////////////
    //计算天猫上架时间
    if (platformID == 1) {
      /////////////////////////////
      //天猫店获取商品的最早的时间并插入数据库，通过评论
      getProductSalesTimeByConment(hotSellIDBroadcast, "TM", sc, sparkID)
      //天猫店获取商品的最早的时间并插入数据库，通过历史
      getProductSalesTimeByHistory(hotSellIDBroadcast, "TM", sc, sparkID)
    }
    //计算京东的上架时间与人口区域分布
    if (platformID == 2) {
      logger.info(" //京东店获取商品的最早的时间并插入数据库，通过评论")
      //京东店获取商品的最早的时间并插入数据库，通过评论
      getProductSalesTimeByConment(hotSellIDBroadcast, "JD", sc, sparkID)
      //京东店获取商品的最早的时间并插入数据库，通过历史
      getProductSalesTimeByHistory(hotSellIDBroadcast, "JD", sc, sparkID)
      //  logger.info("//计算京东的人口分布")
      //计算京东的人口分布
      getJDProductAreaPeople(hotSellIDBroadcast, sc, sparkID)
    }
    //计算一号店上架时间
    if (platformID == 3) {
      logger.info("一号店获取商品的最早的时间并插入数据库，通过采集记录的所有时间")
      //一号店获取商品的最早的时间并插入数据库，通过采集记录的所有时间
      getProductSalesTimeByHistory(hotSellIDBroadcast, "FS", sc, sparkID)
    }

    hotSellIDBroadcast.destroy()
    // UpdateStatus.updateStatus(sparkID, platformID, 1, "任务执行完毕")
    UpdateStatus.updateStatus(sparkID, platformID, 1, "任务执行完毕")
  }


  ///通过评论计算上架的时间
  def getProductSalesTimeByConment(rowKeyPreBroadcast: Broadcast[Array[(String, String)]], tbalePref: String, sc: org.apache.spark.SparkContext, sparkID: String): Unit = {
    logger.info("通过评论计算上架的时间开始" + tbalePref + "CommentDetail")
    val commentRdd = service.getSimpleCommentResult(sc, tbalePref + "CommentDetail")
    val commentResult = commentRdd.filter(r1 => rowKeyPreBroadcast.value.exists(r => r1.rowkey.contains(r._1)))
      .coalesce(28)
      .map {
        r =>
          val rowkey = r.rowkey
          val res = rowKeyPreBroadcast.value.find(r => rowkey.contains(r._1)).getOrElse(("", ""))
          if (res._1 != "") {
            (res._1, r.creationTime)
          } else {
            null
          }
      }.filter(r3 => r3 != null)
    var array = commentResult.reduceByKey((x, y) => if (x < y) x else y).collect()
    SqlServerHelp.getConnection()
    for (item <- array) {
      if (item._1 != null && item._2 != null) {
        val sql =s"""update product set productSalesTime ='${item._2}' where sparkID='${sparkID}' and productID='${item._1}' """
        SqlServerHelp.insertUpdateDelete(sql)
      }
    }
    SqlServerHelp.close
    logger.info("通过评论计算上架的时间" + tbalePref + "CommentDetail 更新完毕")
  }


  ///计算京东各个区域的人数
  /**
    *
    * @param rowKeyPreBroadcast 商品表中各类型top100后的的主键rdd
    * @param sc                 org.apache.spark.SparkContext
    * @param sparkID            当前标记的计算的sparkID
    *
    */
  def getJDProductAreaPeople(rowKeyPreBroadcast: Broadcast[Array[(String, String)]], sc: org.apache.spark.SparkContext, sparkID: String): Unit = {

    logger.info("计算京东各个区域的人数开始：JDCommentDetail");
    val commentRdd = service.commentJDAreaPeopleAddID(sc, "JDCommentDetail")

    val fiterRdd = commentRdd.filter(r => rowKeyPreBroadcast.value.exists(r1 => r.productID.equals(r1._1)))
      .coalesce(28)
      .map {
        r2 =>
          val areaName = if (r2.areaName == null || r2.areaName == "") "null" else r2.areaName
          (r2.productID, areaName)
      }
    val v = (s: String) => {
      mutable.Map(s -> 1)
    }
    val acc = (map: mutable.Map[String, Int], str: String) => {
      if (map.contains(str)) {
        map(str) = map.get(str).get + 1
      }
      else {
        map += (str -> 1)
      }
      map
    }

    val acc1 = (map1: mutable.Map[String, Int], map2: mutable.Map[String, Int]) => {
      val map = mutable.Map[String, Int]()
      map1.foreach(
        r =>
          if (map.contains(r._1)) {
            map(r._1) = map.get(r._1).get + r._2
          } else {
            map += (r._1 -> r._2)
          }
      )
      map2.foreach(
        r =>
          if (map.contains(r._1)) {
            map(r._1) = map.get(r._1).get + r._2
          } else {
            map += (r._1 -> r._2)
          }
      )
      map
    }
    //((),111)
    val res = fiterRdd.combineByKey[mutable.Map[String, Int]](v, acc, acc1).collect()

    SqlServerHelp.getConnection()
    for (elem <- res) {
      val productID = elem._1
      var area = ""
      val builder = new StringBuilder()
      for (item <- elem._2) {
        builder.append(item._1 + ":" + item._2 + ",")
      }
      var str = builder.toString()
      if (str.length > 1) {
        area = str.substring(0, str.length - 1)
      }
      val sql =s"""update product set userProvince ='${area}' where sparkID='${sparkID}' and productID='${productID}' """
      SqlServerHelp.insertUpdateDelete(sql)
    }
    SqlServerHelp.close

    logger.info("计算京东各个区域人数的完毕：JDCommentDetail");
  }

  ///通过采集的最早时间计算上架时间,tbalePref为历史表名称的前缀
  def getProductSalesTimeByHistory(rowKeyPreBroadcast: Broadcast[Array[(String, String)]], tbalePref: String, sc: org.apache.spark.SparkContext, sparkID: String): Unit = {

    logger.info("通过采集的最早时间计算上架时间" + tbalePref + "ProductTimeHistory 开始");
    val fsHistoryRDD = service.getHistory(sc, tbalePref + "ProductTimeHistory")
    var array = fsHistoryRDD.filter(r1 => rowKeyPreBroadcast.value.exists(r => r1.productID.equals(r._1)))
      .coalesce(28)
      .map {
        r =>
          (r.productID, r.downloadTime)
      }.reduceByKey((x, y) => if (x < y) x else y).collect()

    SqlServerHelp.getConnection()
    for (item <- array) {
      if (item._1 != null && item._2 != null) {
        val sql =s"""update product set productSalesTime ='${item._2}' where sparkID='${sparkID}' and productID='${item._1}'  and  ( productSalesTime is null or  productSalesTime='' )"""
        SqlServerHelp.insertUpdateDelete(sql)
      }
    }
    SqlServerHelp.close
    logger.info("通过采集的最早时间计算上架时间" + tbalePref + "ProductTimeHistory 完毕");
  }
}
