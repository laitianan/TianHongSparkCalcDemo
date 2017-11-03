package org.common

import java.time.LocalDate

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{FilterList, SingleColumnValueFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by lta on 2017/3/2.
  */
object HbaseHelp {
  val conf = HBaseConfiguration.create()
  conf.set("hbase.zookeeper.quorum", ConfigurationUntil.hbaseZookeeper)
  conf.set("hbase.zookeeper.property.clientPort", "2181")

  def gethbaseRDD(tableName: String, sc: SparkContext): RDD[(ImmutableBytesWritable, Result)] = {
    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    hBaseRDD
  }


  def gethbaseRDD(tableName: String, sc: SparkContext, beforDay: Int): RDD[(ImmutableBytesWritable, Result)] = {

    var now: LocalDate = LocalDate.now()
    val localDate = now.plusDays(-beforDay).toString()
    val filter1 = new SingleColumnValueFilter(Bytes.toBytes("content"), Bytes.toBytes("downloadTime"), CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(localDate))

   // val filter2 = new SingleColumnValueFilter(Bytes.toBytes("content"), Bytes.toBytes("downloadTime"), CompareOp.LESS_OR_EQUAL, Bytes.toBytes(now.plusDays(1).toString()))

    val filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, filter1)
    val scan = new Scan()
    scan.setFilter(filter1)
    val proto = ProtobufUtil.toScan(scan)
    val ScanToString = Base64.encodeBytes(proto.toByteArray())

    conf.set(TableInputFormat.SCAN, ScanToString)

    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    hBaseRDD
  }


}


////////////////////////////////////////////////////////////////////////////////////////////////////
/*过滤demo
*
*
*
*
    //设置过过滤
    val scan = new Scan()
//    val filter = new SingleColumnValueFilter(Bytes.toBytes("content"), Bytes.toBytes("PageModelKey"), CompareOp.EQUAL, Bytes.toBytes(PageModelKey))
//    scan.setFilter(filter)
//    val proto = ProtobufUtil.toScan(scan)
//    val ScanToString = Base64.encodeBytes(proto.toByteArray())
//
//    conf.set(TableInputFormat.SCAN, ScanToString)
*
* */