import java.time.LocalDate

import scala.reflect.ClassTag
import scala.reflect.Manifest

/**
  * Created by lta on 2017/3/23.
  */
object T {

  case class Person(name: String, val age: Int = 12)

  class Person2(name: String, val age: Int = 12,var b:Boolean=false) {
    r=>
    def this(age: Int,boo:Boolean) {
      this("lta", age)
      println(r.name+":"+this.age+":"+r.b)
      r.b=true
    }
  }

  def fun(g: Any): Int = 0

  def main(args: Array[String]): Unit = {
    val localDate = println(LocalDate.now().plusDays(-2).toString)

    val s = Person("5", 12)
    val ss = new Person2( 12,false)
    println(10)
    println(fun(0))
    val ff: Any => Int = {
      r =>
        "s" match {
          case "s" => 12
          case "ss" => 122
          case _ => 156
        }
    }

    val g: Any => Int = ff


    println(g(10))
    println(11)
  }

  def foo[T](x: List[T])(implicit m: Manifest[T]) = {
    if (m <:< manifest[String]) {
      println("Hey, this list is full of strings")
      println(123333)
    }
    else {
      println("Non-stringy list")
    }
  }

  def main2(args: Array[String]): Unit = {


    //
    //    val list = List("1", "2", "3", "4", "5", "5", "6")
    //    println(list.exists(r => r.equals(null)))

    //    import TestObject.Tc
    //    val tc: Tc = new Tc("12")
    //
    //    val tcClass = new TestClass("12")
    //    val Triple1=new Triple[TestClass,Int,Int](tc,12,12)


  }
}

class Triple1[F: ClassTag, S, T](val first: F, val second: S, val third: T) {

}

class TestClass(a: String)

object TestObject {
  type Tc = TestClass
}





