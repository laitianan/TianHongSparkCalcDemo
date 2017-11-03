/**
  * Created by lta on 2017/3/4.
  */

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.common.{ConfigurationUntil, HbaseHelp, SqlServerHelp}
import org.tianhong.model.History

import scala.reflect.ClassTag

case class SimpleCommentAreaPeople(val rowkey: String, areaName: String)

case class SimpleCommentAreaPeopleAddID(productID: String, areaName: String)

object JDPeopleArea {


  private val conf = HBaseConfiguration.create()
  conf.set("hbase.zookeeper.quorum", ConfigurationUntil.hbaseZookeeper)
  conf.set("hbase.zookeeper.property.clientPort", "2181")

  private var sc: org.apache.spark.SparkContext = null
  private var rddCache: RDD[SimpleCommentAreaPeople] = null

  def setSparkContext(sparkContext: org.apache.spark.SparkContext): Unit = {
    sc = sparkContext
    setTable(sc, "JDCommentDetail")
  }

  def getProductAreaPeople(rowKeyPreRdd: Array[String], sparkID: String): Unit = {
    SqlServerHelp.getConnection()
    for (elem <- rowKeyPreRdd) {
      val productID = elem
      if (elem != null) {
        val length = elem.length

        val areaPeopleResult = rddCache.filter(r => r.rowkey.contains(elem))
          .map {
            r1 => (r1.areaName, 1)
          }
        var array = areaPeopleResult.reduceByKey(_ + _).collect()
        var strValue: String = ""
        for (t <- array) {
          strValue = t._1 + ":" + t._2 + ","
        }
        if (strValue.length > 2) strValue = strValue.substring(0, strValue.length - 1)

        //        if (strValue.length > 0) {
        //          val sql =s"""update product set userProvince ='${strValue}' where sparkID='${sparkID}' and productID='${productID}' """
        //          SqlServerHelp.insertUpdateDelete(sql)
        //        }
        println(productID + "----" + strValue)
      }
    }

    SqlServerHelp.close
  }

  private def setTable(sc: SparkContext, tableName: String): Unit = {
    val rdd = gethbaseRDD(tableName, sc)
    val commentAreaPeople = rdd.map {
      r => {
        val rowKey = Bytes.toString(r._2.getRow)
        val userProvince = Bytes.toString(r._2.getValue(Bytes.toBytes("content"), Bytes.toBytes("userProvince")))
        SimpleCommentAreaPeople(rowKey, userProvince)
      }
    }
    rddCache = commentAreaPeople
    rddCache.persist()
  }


  private def gethbaseRDD(tableName: String, sc: SparkContext): RDD[(ImmutableBytesWritable, Result)] = {
    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    hBaseRDD
  }

}
