
object T {

  def main(args: Array[String]): Unit = {
    val increMap = collection.mutable.HashMap[Int,Int]()
    increMap(1) = increMap.getOrElse(1,0)+1
    increMap(1) = increMap.getOrElse(1,3)+1
    print(increMap)
  }

}



