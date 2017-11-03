//import java.text.SimpleDateFormat
//import java.util.{Calendar, Date}
//
//import org.apache.spark.sql.SparkSession
//import org.common.{PropertiesHelp, SqlServerHelp}
//import org.tianhong.TianHongCalc

/**
  * Created by lta on 2017/3/3.
  */
object Test {
  def main(args: Array[String]): Unit = {
    //    SqlServerHelp.getConnection()
    //    val str = "select * from  history_offset"
    //    val result = SqlServerHelp.select(str)
    //    while (result.next()) {
    //      println(result.getTimestamp("create_time"))
    //    }
    //    SqlServerHelp.close

    //    val stringBuilder=new StringBuffer("58588d167dc6b211d8c28a1a").reverse();
    //    println(stringBuilder)

    //    val Pattern = "(\\d+)".r
    //
    //    val s="920"
    //
    //    s match  {
    //      case Pattern(ss)=>println(ss)
    //      case _=>println("none")
    //    }


    //    val date = new Date();//获取当前时间
    //    val calendar = Calendar.getInstance();
    //    calendar.setTime(date);
    //    calendar.add(Calendar.DAY_OF_MONTH, -1);//当前时间减去一年，即一年前的时间
    //  //  calendar.add(Calendar.MONTH, -1);//当前时间前去一个月，即一个月前的时间
    //    ;//获取一年前的时间，或者一个月前的时间
    //    var sim = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
    //
    //    val dateStr = sim.format(calendar.getTime())
    //    println(dateStr)

    //    val spark = SparkSession.builder().master("local").appName("HBaseTest").getOrCreate()
    //    import spark.implicits._
    //    val sc = spark.sparkContext
    //
    //    val rdd=sc.parallelize(List("fdfd","fdfd",null,"45546"))
    //    rdd.filter(r=>r!=null && r.contains("4")).foreach(r=>println(r+"--"))
    //
    //    val service = new TianHongCalc()
    //    val hotShellResultRdd = service.getResultHotShellResult(sc, "TMHotShell","78")
    //    val sourceDF = hotShellResultRdd.toDF().cache()
    //    sourceDF.createOrReplaceTempView("ProductTable")
    //    sourceDF.printSchema()
    //
    //    sourceDF.show()

    //    val sqlContext = spark.sqlContext
    //    val sql ="""select *  from ProductTable  """
    //    val DF = sqlContext.sql(sql).show()
    //
    //        hotShellResultRdd.toDF().write.mode("append").jdbc("jdbc:mysql://192.168.10.86:3306/beyebe_bigdata", "hotsell", PropertiesHelp.getProperties())

    //    SqlServerHelp.getConnection()
    //    val sql =s"""update product set productSalesTime ='1221331' where sparkID='454646464' and productID='45464646' """
    //    SqlServerHelp.insertUpdateDelete(sql)
    //    SqlServerHelp.close

    def g(i: Int): Int = {
      return i;
    }

    def gg(f: (Int) => Int) = {
      println(f(12))
    }

    def saySomething(prefix: String) = (s: String) => {
      prefix + " " + s
    }


    def saySomething2(prefix: String) = (s: String) => {
      prefix + " " + s
    }


    val sayChinaese = (s: String) => {
      println(s)
      s
    }


    val sayChinaese2: (String, (String, Int)) => String = {
      case (x, (x1, y)) =>
        println(x)
        println(y * 10)
        x1
    }


    val sayChinaese3: (String, (String, Int)) => String = {
      case (x, (x1, y)) =>
        println(x)
        println(y * 10)
        x1
    }


    val sayHello = saySomething("Hello")
    print(sayHello("lta"))
    println(sayChinaese2("lta", ("kkk", 12)))
    val k = 77123;

    //assert(false, println(k))
    print(12)
    val english = () => "Hello, "


    val f = (l: List[Int]) => l.mkString("")
    val ga: (AnyVal) => String = {
      case i: Int => "Int"
      case d: Double => "Double"
      case o => "Other"
    }


    println(ga(10))

    val fa = () => {
      return "test"
    }
    //    println(fa())


    def faa: String = {
      val gaa = () => {
        return "test2"
      }
      gaa()
      "not this"
    }

    //    print(faa)
    val list = List(12, 1, 0, 5, 0, 3, 4, 5)
    val ret = list.aggregate("0")({ (a, b) => a + "*****" + (b * 20).toString }, { (a, b) => a + b })


    println(ret)



    println(list.fold(10)((a, b) => a + b))


    println(list.sortWith((r1, r2) => r1 > r2))


  }
}
