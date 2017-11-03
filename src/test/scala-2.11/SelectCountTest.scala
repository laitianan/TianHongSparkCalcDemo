import java.util.UUID

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession
import org.common.HbaseHelp.conf
import org.common.{ConfigurationUntil, HbaseHelp}
import org.tianhong.TianHongCalc


        object SelectCountTest {
          def main(args: Array[String]): Unit = {

            var sparkID = UUID.randomUUID().toString
            val build = SparkSession.builder().appName("TianHongCalc-" + sparkID) // .master("local")
            if (!(System.getProperty("os.name").toLowerCase.contains("linux"))) {
              build.master("local")
            }

            val spark: SparkSession = build.getOrCreate()
            import  spark.implicits._
            val conf = HBaseConfiguration.create()
            conf.set("hbase.zookeeper.quorum", ConfigurationUntil.hbaseZookeeper)
            conf.set("hbase.zookeeper.property.clientPort", "2181")
            conf.set(TableInputFormat.INPUT_TABLE, "JDProductDetail")
            val hBaseRDD = spark.sparkContext.newAPIHadoopRDD(conf, classOf[TableInputFormat],
              classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
              classOf[org.apache.hadoop.hbase.client.Result])

           // val rdd=HbaseHelp.gethbaseRDD("JDProductDetail", spark.sparkContext)
            val ret=hBaseRDD.map{
              r=>
                val list=new scala.collection.mutable.MutableList[String]()
                val scan=r._2.cellScanner()
                while (scan.advance()){
                  val ret=scan.current().getValue
                  if (null==ret){list.+=("NULL")}
                  else {
                    list.+=(Bytes.toString(ret))
                  }

                }
                println(list)
                list
            }
            ret.collect().foreach(println)

          }
        }


/*
*
*
* //            val sql=
//              """
//                 select * from (
//                 select downloadTime,*, dense_rank() OVER (PARTITION BY  productBrand ORDER BY productPrice desc) as rank from ProductTable ) where rank<=10 order by productBrand ,productPrice desc
//              """.stripMargin
*
* */