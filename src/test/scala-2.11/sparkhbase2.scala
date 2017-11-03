import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{FilterList, PageFilter, SingleColumnValueFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.common.ConfigurationUntil

import scala.collection.SortedMap
import util.control.Breaks._

/**
  * Created by Kratos on 2017/7/27.
  */
object sparkhbase2 {

  //  val logger = Logger.getLogger(org.apache.spark.sql.SparkSession.getClass).setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", ConfigurationUntil.hbaseZookeeper)
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "JDProductDetail")

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("OFF")

    val startTime1 = System.currentTimeMillis()
    val hBaseRDD = spark.sparkContext.newAPIHadoopRDD(hbaseConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]).map { r =>
      val list = new scala.collection.mutable.MutableList[String]()
      val scan = r._2.cellScanner()
      while (scan.advance()) {
        val ret = scan.current().getValue
        if (null == ret) {
          list.+=("NULL")
        }
        else {
          list.+=(Bytes.toString(ret))
        }
        println(list)
        list
      }
      list
    }.collect()


  }
}
