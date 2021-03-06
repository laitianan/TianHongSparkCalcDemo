package org.tianhong

import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession
import org.common._
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.rdd.RDD
import org.tianhong.model._
import org.apache.spark.SparkContext

import scala.util.matching.Regex

/**
  * Created by lta on 2017/3/2.
  */


class TianHongCalc {


  def getResultProductDetail(sc: SparkContext, platformI: Int, tableName: String, beforDay: Int, sparkIDs: String): RDD[ProductDetailModel] = {
    val rddV = HbaseHelp.gethbaseRDD(tableName, sc, beforDay)
    val platformID: Int = platformI
    val sparkID = sparkIDs
    val rdd = rddV.map[ProductDetailModel] {
      r => {
        val productID = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("productID")))
        val productName = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("productName")))
        val productBrand = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("productBrand")))


        val productSpec = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("productSpec")))

        val productPriceStr: String = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("productPrice")))

        val productPrice = if (productPriceStr == null || productPriceStr == "" || productPriceStr == "null" ) {
          0.0
        } else {
          productPriceStr.toDouble
        }

        val commentCountStr = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("commentCount")))
        val commentCount: Int =
          if (commentCountStr == null || commentCountStr == "" || commentCountStr == "null") {
            0
          } else {
            commentCountStr.toInt
          }

        val enshrineCountStr = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("enshrineCount")))
        val enshrineCount: Int = if (enshrineCountStr == null || enshrineCountStr == "" || enshrineCountStr == "null") {
          0
        } else {
          enshrineCountStr.toInt
        }

        val weeklySalesStr = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("weeklySales")))
        val weeklySales: Int =
          if (weeklySalesStr == null || weeklySalesStr == "" || weeklySalesStr == "null") {
            0
          } else {
            weeklySalesStr.toInt
          }

        val Pattern = "\\d+".r
        val monthlySalesStrOrNull = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("monthlySales")))
        val monthlySales = monthlySalesStrOrNull match {
          case Pattern() => monthlySalesStrOrNull.toInt
          case _ => 0
        }

        // val monthlySales: Int = monthlySalesStr.toInt


        val salesEvents = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("salesEvents")))
        val category = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("category")))
        val standardCategory = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("standardCategory")))
        var downloadTime = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("downloadTime")))
        downloadTime =
          if (downloadTime == null || downloadTime == "") "3000-12-30T00:00:00.4827679+08:00" else
            downloadTime


        val productHotSellID = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("productHotSellID")))

        val productArea = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("productArea")))
        val fitPeople = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("fitPeople")))
        val productCode = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("productCode")))
        val categoryPath = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("categoryPath")))

        val indexStr = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("index")))
        var index: Int = 0
        if (indexStr == null || indexStr == "" || indexStr == "null") {
          0
        } else {
          index = indexStr.toInt
        }


        val isWorldWide = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("isWorldWide")))


        val groupBuying = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("groupBuying")))

//        println(productID+"---"+platformID+"---"+productName+"---"+productBrand+"---"+productSpec+"---"+productPrice+"---"+commentCount+"---"+enshrineCount+"---"+weeklySales+"---"+index+"---"+isWorldWide+"---"+groupBuying+"---"+sparkID)

        ProductDetailModel(productID, platformID, productName, productBrand, productSpec, productPrice, commentCount, enshrineCount, weeklySales, monthlySales, salesEvents, category, standardCategory, downloadTime, productHotSellID, productArea, fitPeople, productCode, categoryPath, index, isWorldWide, groupBuying, sparkID)



      }
    }
    rdd
  }

  def getResultProductDetail(sc: SparkContext, platformI: Int, tableName: String, sparkIDs: String): RDD[ProductDetailModel] = {
    val rddV = HbaseHelp.gethbaseRDD(tableName, sc)
    val platformID: Int = platformI
    val sparkID = sparkIDs
    val rdd = rddV.map[ProductDetailModel] {
      r => {
        val productID = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("productID")))
        val productName = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("productName")))
        val productBrand = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("productBrand")))


        val productSpec = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("productSpec")))

        val productPriceStr: String = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("productPrice")))

        val productPrice = if (productPriceStr == null || productPriceStr == "" || productPriceStr == "null" || productPriceStr == "null") {
          0.0
        } else {
          productPriceStr.toDouble
        }

        val commentCountStr = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("commentCount")))
        val commentCount: Int =
          if (commentCountStr == null || commentCountStr == "" || commentCountStr == "null") {
            0
          } else {
            commentCountStr.toInt
          }

        val enshrineCountStr = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("enshrineCount")))
        val enshrineCount: Int = if (enshrineCountStr == null || enshrineCountStr == "" || enshrineCountStr == "null") {
          0
        } else {
          enshrineCountStr.toInt
        }

        val weeklySalesStr = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("weeklySales")))
        val weeklySales: Int =
          if (weeklySalesStr == null || weeklySalesStr == "" || weeklySalesStr == "null") {
            0
          } else {
            weeklySalesStr.toInt
          }

        val Pattern = "\\d+".r
        val monthlySalesStrOrNull = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("monthlySales")))
        val monthlySales = monthlySalesStrOrNull match {
          case Pattern() => monthlySalesStrOrNull.toInt
          case _ => 0
        }

        // val monthlySales: Int = monthlySalesStr.toInt


        val salesEvents = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("salesEvents")))
        val category = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("category")))
        val standardCategory = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("standardCategory")))
        var downloadTime = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("downloadTime")))
        downloadTime = if (downloadTime == null || downloadTime == "") "3000-12-30T00:00:00.4827679+08:00" else downloadTime


        val productHotSellID = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("productHotSellID")))

        val productArea = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("productArea")))
        val fitPeople = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("fitPeople")))
        val productCode = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("productCode")))
        val categoryPath = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("categoryPath")))

        val indexStr = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("index")))
        var index: Int = 0
        if (indexStr == null || indexStr == "" || indexStr == "null") {
          0
        } else {
          index = indexStr.toInt
        }


        val isWorldWide = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("isWorldWide")))


        val groupBuying = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("groupBuying")))

        ProductDetailModel(productID, platformID, productName, productBrand, productSpec, productPrice, commentCount, enshrineCount, weeklySales, monthlySales, salesEvents, category, standardCategory, downloadTime, productHotSellID, productArea, fitPeople, productCode, categoryPath, index, isWorldWide, groupBuying, sparkID)
      }
    }
    rdd
  }

  def getResultHotShellResult(sc: SparkContext, tableName: String, sparkid: String, platform: Int): RDD[HotShell] = {
    val rdd = HbaseHelp.gethbaseRDD(tableName, sc)
    val sparkID = sparkid
    val platformID = platform
    val hotShellRDD = rdd.map {
      r => {
        //  val rowKey=Bytes.toString(r._2.getRow);
        val productHotSellID = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("productHotSellID")))
        var productID: String = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("productID")))
        var productName: String = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("productName")))
        var productSpec: String = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("productSpec")))
        var productPriceStr: String = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("productPrice")))
        val productPrice: Double = if (productPriceStr == null || productPriceStr == "" || productPriceStr == "null") {
          0.0
        } else {
          productPriceStr.toDouble
        }
        var productURL: String = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("productURL")))
        var downloadTime = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("downloadTime")))
        downloadTime = if (downloadTime == null || downloadTime == "") "3000-12-30T00:00:00.4827679+08:00" else downloadTime


        var productImage: String = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("productImage")))
        HotShell(productHotSellID, productID, productName, productSpec, productPrice, productURL, downloadTime, productImage, platformID, sparkID)
      }
    }
    hotShellRDD
  }

  def getSimpleCommentResult(sc: SparkContext, tableName: String): RDD[SimpleComment] = {

    val rdd = HbaseHelp.gethbaseRDD(tableName, sc)

    val commentRDD = rdd.map {
      r => {
        var creationTime = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("creationTime")))

        creationTime = if (creationTime == null || creationTime == "") "3000-12-30T00:00:00.4827679+08:00" else creationTime
        val id = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("id")))
        val rowKey = Bytes.toString(r._2.getRow)
        SimpleComment(rowKey, id, creationTime)
      }
    }
    commentRDD
  }


  def getHistory(sc: SparkContext, tableName: String): RDD[History] = {
    val rdd = HbaseHelp.gethbaseRDD(tableName, sc)
    val fsHistoryRDD = rdd.map {
      r => {
        val rowKey = Bytes.toString(r._2.getRow)
        val productID = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("productID")))
        var downloadTime = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("downloadTime")))
        downloadTime = if (downloadTime == null || downloadTime == "") "3000-12-30T00:00:00.4827679+08:00" else downloadTime
        History(rowKey, productID, downloadTime)
      }
    }
    fsHistoryRDD
  }

  def commentJDAreaPeople(sc: SparkContext, tableName: String): RDD[SimpleCommentAreaPeople] = {
    val rdd = HbaseHelp.gethbaseRDD(tableName, sc)
    val commentAreaPeople = rdd.map {
      r => {
        val rowKey = Bytes.toString(r._2.getRow)
        val userProvince = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("userProvince")))
        SimpleCommentAreaPeople(rowKey, userProvince)
      }
    }
    commentAreaPeople
  }

  def commentJDAreaPeopleAddID(sc: SparkContext, tableName: String): RDD[SimpleCommentAreaPeopleAddID] = {
    val rdd = HbaseHelp.gethbaseRDD(tableName, sc)
    val commentAreaPeople = rdd.map {
      r => {
        // val rowKey = Bytes.toString(r._2.getRow)
        val productID = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("productID")))
        val userProvince = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("userProvince")))
        SimpleCommentAreaPeopleAddID(productID, userProvince)
      }
    }
    commentAreaPeople
  }
}
