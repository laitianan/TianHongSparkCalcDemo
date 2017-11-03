import scala.reflect.Manifest

/**
  * Created by lta on 2017/8/7.
  */
object mainfest {
  def isbool = false

  def main(args: Array[String]): Unit = {
    foo(List("one", "two")) // Hey, this list is full of strings
    foo(List(1, 2)) // Non-stringy list
    foo(List("one", 2)) // Non-stringy list

  }


  def foo[T](x: List[T])(implicit m: Manifest[T]) = {
    if (m <:< manifest[String]) {
      println("Hey, this list is full of strings")
    }
    else {
      println("Non-stringy list")

    }
  }
}
