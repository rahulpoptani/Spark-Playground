package learnScala

object ScalaCollections {
  def main(args: Array[String]): Unit = {
    val l1 = List(1, 2, 3, 4)
    val l2 = List(5, 6, 7, 8)
    l1.zip(l2).foreach(println)

    val seq1 = Seq((1,2,3),(4,5,6))
    val seq2 = seq1 ++ Seq((7,8,9),(10,11,12))

    print(seq2)
  }
}
