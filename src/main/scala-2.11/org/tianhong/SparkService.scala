package org.tianhong


import java.text.SimpleDateFormat

import org.common.{PropertiesHelp, SqlServerHelp, UpdateStatus}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import java.util.{Calendar, Date, Properties}
import java.util.regex.Pattern

import org.apache.log4j.Logger
import org.apache.spark.storage.StorageLevel

/**
  * Created by lta on 2017/3/3.
  */

/*
* val spark = SparkSession.builder().master("local").appName("HBaseTest").getOrCreate()

* */
object SparkService {
  val logger = Logger.getLogger(SparkService.getClass);
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
    val result = service.getResultProductDetail(sc, platformID, tableName, sparkID)
    val sourceDF = result.toDF().cache()
    logger.info("缓存表数据" + tableName)
    // sourceDF.show()
    sourceDF.createOrReplaceTempView("ProductTable")
    logger.info("注册ProductTable")
    // sourceDF.printSchema()

    //  sourceDF.show()
    val sqlContext = spark.sqlContext

    val date = new Date()
    //获取当前时间
    val calendar = Calendar.getInstance()
    calendar.setTime(date)
    calendar.add(Calendar.DAY_OF_MONTH, -nDay)
    //当前时间减去多少N天
    var sim = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")

    val dateStr = sim.format(calendar.getTime())

   // val list = List("洗发")
    val list = List("洗发", "护发", "沐浴露", "牙膏", "护肤品", "日本进口零食")
   // val list = List("牙膏")
    //val list = List("洗发")
    var allRdd: DataFrame = null
    for (item <- list) {

      var sortFie = ""
      if (platformID == 1) {
        sortFie = "monthlySales"
      }
      else if ((platformID == 2 || platformID == 3) && !item.equals("日本进口零食")) {
        sortFie = "index"
      } else {
        sortFie = "commentCount"
      }

      val sql =
        s"""select *  from ProductTable  where standardCategory  = '${item}'  and downloadTime>'${dateStr}' and productPrice>0 and productSpec !='此商品已下架' order by ${sortFie} desc limit 100
        """.stripMargin
      logger.info("sparkSql查询数据语句" + sql)
      val DF = sqlContext.sql(sql)
      logger.info("sparkSql查询数据语句执行")
      println(item + "-----------------------------------")
     //  DF.show()
      if (allRdd == null) {
        allRdd = DF
      } else {
        allRdd = allRdd.union(DF)
        logger.info("整个表执行Union：" + item)
      }
    }


    logger.info("整个表追加数据到关系数据库：" + tableName)
    allRdd.write.mode("append").jdbc("jdbc:mysql://192.168.10.86:3306/beyebe_bigdata", "product", PropertiesHelp.getProperties())
    logger.info("整个表追加数据到关系数据库完毕：" + tableName)
    val hotSellIDArray = allRdd.map {
      r =>
        r.getAs[String]("productHotSellID")
    }.collect()


    logger.info("获取表：" + tableName + "的爆品关联表rdd:" + preF + "HotShell" + "并且缓存")
    val hotShellResultRdd = service.getResultHotShellResult(sc, preF + "HotShell", sparkID,platformID).persist(StorageLevel.MEMORY_AND_DISK)
    logger.info("获取表：" + tableName + "的爆品关联表rdd:" + preF + "HotShell 完毕")
    var hotShellResul = null
    //爆品关联
    for (elem <- hotSellIDArray) {
      if (elem != null) {
        logger.info(s"表:${tableName}:, productID:${elem}开始查找关联爆品并导入")
        val hotShellResul = hotShellResultRdd.
          filter(r => (r.productHotSellID != null && r.productHotSellID.equals(elem)))
          .toDF()
        hotShellResul.write.mode("append").jdbc("jdbc:mysql://192.168.10.86:3306/beyebe_bigdata", "hotsell", PropertiesHelp.getProperties())
        logger.info(s"表:${tableName}:, productID:${elem}关联爆品导入完毕")
      }
    }

    hotShellResultRdd.unpersist()
    logger.info("爆品表释放缓存")

    val rowKeyPreRdd = allRdd
      .map {
        r =>
          val productID = r.getAs[String]("productID")
          productID
      }.collect()
    logger.info("获取整表的rowkey的RDD")

    /////////////////////////
    //计算天猫上架时间
    if (platformID == 1) {
      logger.info("天猫店获取商品的最早的时间并插入数据库，通过评论")
      /////////////////////////////
      //天猫店获取商品的最早的时间并插入数据库，通过评论
      getProductSalesTimeByConment(rowKeyPreRdd, "TM", sc, sparkID)

      getProductSalesTimeByHistory(rowKeyPreRdd, "TM", sc, sparkID)

    }
    //计算京东的上架时间与人口区域分布
    if (platformID == 2) {
      logger.info(" //京东店获取商品的最早的时间并插入数据库，通过评论")
      //京东店获取商品的最早的时间并插入数据库，通过评论
      getProductSalesTimeByConment(rowKeyPreRdd, "JD", sc, sparkID)

      getProductSalesTimeByHistory(rowKeyPreRdd, "JD", sc, sparkID)
      logger.info("//计算京东的人口分布")
      //计算京东的人口分布
      getJDProductAreaPeople(rowKeyPreRdd, sc, sparkID)
    }
    //计算一号店上架时间
    if (platformID == 3) {
      logger.info("一号店获取商品的最早的时间并插入数据库，通过采集记录的所有时间")
      //一号店获取商品的最早的时间并插入数据库，通过采集记录的所有时间
      getProductSalesTimeByHistory(rowKeyPreRdd, "FS", sc, sparkID)
    }
    UpdateStatus.updateStatus(sparkID, platformID, 1, "任务执行完毕")
  }


  ///通过评论计算上架的时间
  def getProductSalesTimeByConment(rowKeyPreRdd: Array[String], tbalePref: String, sc: org.apache.spark.SparkContext, sparkID: String): Unit = {

    val commentRdd = service.getSimpleCommentResult(sc, tbalePref + "CommentDetail").persist(StorageLevel.MEMORY_AND_DISK)
    println("----------------------------------")
    //commentRdd.take(1600).foreach(r=>println(r))

    SqlServerHelp.getConnection()

    for (elem <- rowKeyPreRdd) {

      if (elem != null) {
        val length = elem.length
        val commentResult = commentRdd.filter(r => r.rowkey.contains(elem)).map {
          r1 => (r1.rowkey.substring(0, length), r1.creationTime)
        }
        var array = commentResult.reduceByKey((x, y) => if (x < y) x else y).collect()

        for (item <- array) {
          if (item._1 != null && item._2 != null) {
            val sql =s"""update product set productSalesTime ='${item._2}' where sparkID='${sparkID}' and productID='${item._1}'  and   productSalesTime is null  """
            SqlServerHelp.insertUpdateDelete(sql)
          }
        }
      }

    }
    commentRdd.unpersist()
    SqlServerHelp.close
  }


  ///计算京东各个区域的人数
  /**
    *
    * @param rowKeyRdd   商品表中各类型top100后的的主键rdd
    * @param sc  org.apache.spark.SparkContext
    * @param sparkID  当前标记的计算的sparkID
    *
    */
  def getJDProductAreaPeople(rowKeyRdd: Array[String], sc: org.apache.spark.SparkContext, sparkID: String): Unit = {

    SqlServerHelp.getConnection()
    val commentRdd = service.commentJDAreaPeople(sc, "JDCommentDetail").persist(StorageLevel.MEMORY_AND_DISK)
    for (elem <- rowKeyRdd) {
      val productID = elem
      if (elem != null) {
        val areaPeopleResult = commentRdd.filter(r => r.rowkey.contains(elem))
          .map {
            r1 => (r1.areaName, 1)
          }
        //areaPeopleResult.foreach(r=>println(r._1))
        var array = areaPeopleResult.reduceByKey(_ + _).collect()
        if (array.length > 0) {
          var strValue: String = ""
          for (t <- array) {
            strValue = strValue+t._1 + ":" + t._2 + ","
          }
          if (strValue.length > 2) strValue = strValue.substring(0, strValue.length - 1)

          if (strValue.length > 0) {
            val sql =s"""update product set userProvince ='${strValue}' where sparkID='${sparkID}' and productID='${productID}' """
            SqlServerHelp.insertUpdateDelete(sql)
          }
         // println(productID + "----" + strValue)
        }
      }
    }

    SqlServerHelp.close


  }


  ///通过采集的最早时间计算上架时间,tbalePref为历史表名称的前缀
  def getProductSalesTimeByHistory(rowKeyPreRdd: Array[String], tbalePref: String, sc: org.apache.spark.SparkContext, sparkID: String): Unit = {
    val fsHistoryRDD = service.getHistory(sc, tbalePref + "ProductTimeHistory").persist(StorageLevel.MEMORY_AND_DISK)
    SqlServerHelp.getConnection()
    for (elem <- rowKeyPreRdd) {
      if (elem != null) {
        val length = elem.length
        val historyResult = fsHistoryRDD.filter(r => r.rowKey.contains(elem)).map {
          r1 => (r1.rowKey.substring(0, length), r1.downloadTime)
        }
        var array = historyResult.reduceByKey((x, y) => if (x < y) x else y).collect()
        for (item <- array) {

          if (item._1 != null && item._2 != null) {
       //     println(sparkID + "--" + item._1 + "--" + item._1)
            val sql =s"""update product set productSalesTime ='${item._2}' where sparkID='${sparkID}' and productID='${item._1}' and productSalesTime is null """
            SqlServerHelp.insertUpdateDelete(sql)
          }
        }
      }

    }
    fsHistoryRDD.unpersist()
    SqlServerHelp.close
  }
}
