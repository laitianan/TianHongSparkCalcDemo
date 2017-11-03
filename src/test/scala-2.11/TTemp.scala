


class Pair[T <: Comparable[T]](val first: T, val second: T) {
  def bigger = if (first.compareTo(second) > 0) first else second
}

// 声明带T泛型参数的类
class Pair_Lower_Bound[T](val first: T, val second: T) {
  def replaceFirst[R >: T](newFirst: R) = new Pair_Lower_Bound[R](newFirst, second)
}


object TTemp {

  def main(args: Array[String]): Unit = {
    var pair = new Pair("Spark", "Hadoop")
    println(pair.bigger)
  }

}