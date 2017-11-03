import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{CompareFilter, PageFilter, SingleColumnValueFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.common.ConfigurationUntil

import scala.collection.mutable

/**
  * Created by lta on 2017/3/4.
  */
object TestT {


  def main(args: Array[String]): Unit = {
    val build = SparkSession.builder().appName("TianHongCalc-TEST") // .master("local")
    if (!(System.getProperty("os.name").toLowerCase.contains("linux"))) {
      build.master("local")
    }
    val spark: SparkSession = build.getOrCreate()
    val sc = spark.sparkContext

    val columnName = "DownloadTime"
    val standardCategoryName = "护发"
    val limit = 2;
    val filterSingleColumn = new SingleColumnValueFilter("content".getBytes, "DownloadTime".getBytes, CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(1493568000000l))

    filterSingleColumn.setFilterIfMissing(true);
    val limitFilter = new PageFilter(limit);
    val list = List(filterSingleColumn, limitFilter)
    val scan = new Scan();
    //scan.setMaxResultSize(10)
    scan.setFilter(filterSingleColumn);
    //scan.setFilter(limitFilter);
    scan.setCaching(100)

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", ConfigurationUntil.hbaseZookeeper)
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    val tableName = "mongodb:www.tianyancha.com"


    conf.set(TableInputFormat.INPUT_TABLE, tableName)

    val proto = ProtobufUtil.toScan(scan)
    val ScanToString = Base64.encodeBytes(proto.toByteArray())

    conf.set(TableInputFormat.SCAN, ScanToString)

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    println("---------------------------------------------------------------------------")
    val s = hBaseRDD.map {
      r =>
        "1"
    }
    println(s.collect().length)

    sc.stop()

  }


  def main2(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("HBaseTest").getOrCreate()

    val rdd = spark.sparkContext.parallelize(
      List(("1", "广州"), ("1", "茂名"), ("1", "广州"), ("1", "深圳"), ("1", "深圳"), ("1", "广州"), ("1", "广州"), ("2", "福建"), ("2", "泉州"), ("2", "福建"), ("2", "福建"), ("2", "泉州"), ("1", "null"), ("1", "null")))
    //    JDPeopleArea.setSparkContext(spark.sparkContext)
    //    JDPeopleArea.getProductAreaPeople(Array("1606119206"),"")

    val v = (s: String) => {
      //      var strcopy=s
      //      if(strcopy==null)
      //        strcopy="null"
      mutable.Map(s -> 1)
    }

    val acc = (map: mutable.Map[String, Int], str: String) => {
      //      var strcopy=str
      //      if(strcopy==null)
      //        strcopy="null"
      if (map.exists(r => r._1.equals(str))) {
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
          if (map.exists(r1 => r1._1.equals(r._1))) {
            map(r._1) = map.get(r._1).get + r._2
          } else {
            map += (r._1 -> r._2)
          }
      )
      map2.foreach(
        r =>
          if (map.exists(r1 => r1._1.equals(r._1))) {
            map(r._1) = map.get(r._1).get + r._2
          } else {
            map += (r._1 -> r._2)
          }
      )
      map
    }
    //    val a = v("name")
    //    println(a)
    //    val b = acc(a, "name3")
    //    println(b)
    //    val c = acc(a, "name2")
    //    println(c)

    val res = rdd.combineByKey[mutable.Map[String, Int]](v, acc, acc1)

    res.foreach(r => println(r))

  }

  def re(s: String): String = {
    s
  }
}
