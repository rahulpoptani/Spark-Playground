package learnScala

import scala.annotation.tailrec

object LearnScala {

  def main(args: Array[String]): Unit = {

    // calling an function
    cube(3)

    // Pattern Matching
    val numPattern = "[0-9]+".r // Here, .r means it's an regix
    val address = "123 Main Street 101"
    val match1 = numPattern.findFirstIn(address)
    val match2 = numPattern.findAllIn(address) // returns match iterator
    println(s"First Matching Pattern: ${match1.getOrElse("no matching")}")
    println(s"Second Matching Pattern:")
    match2.foreach(println)

    // Another approach
    println("Printing via match approach")
    match1 match {
      case Some(x) => println(x)
      case None =>
    }

    // Search via regular expression and replace the characters
    val match3 = numPattern.replaceAllIn(address, "x")
    println(s"Address String After Replace: ${match3}")

    // Get a character at specific position. Two approaches
    val sampleString = "sampleString"
    println(s"String: ${sampleString} | Character at 2nd index is ${sampleString.charAt(2)} | and 3rd index is ${sampleString(3)}")

    // Generate random
    val rand = scala.util.Random
    println(s"Random String: ${rand.nextString(5)}") // prints chinese characters :)
    println(s"Random Int Unbounded: ${rand.nextInt}")
    println(s"Random Int Bounded: ${rand.nextInt(4)}")

    // Run multiple counter FOR loop
    for {
      i <- 1 to 3
      j <- 1 to 2
    } println(s"i=$i j=$j")

    // Implementing nested breaks
    println("Implementing Breaks in Scala")
    import scala.util.control._
    val Inner = new Breaks
    val Outer = new Breaks
    Outer.breakable {
      for (i <- 1 to 5) {
        Inner.breakable {
          for (j <- 'a' to 'e') {
            if (i == 1 && j == 'c') Inner.break else println(s"i=$i;j=$j")
            if (i == 2 && j == 'b') Outer.break
          }
        }
      }
    }

    // Implementing tail recursion
    println("Implementing tail recursion")
    println(s"Factorial of 5 is ${factorial(5)}")

    // Get class of any type
    println("Get class of any type")
    val i = 20
    val s = "abc"
    println(getClassAsString(i))
    println(getClassAsString(s))


  }

  def cube(n: Int): Unit = {
    println(s"Cube root of ${n} is ${n * n * n}")
  }

  def factorial(n: Int): Int = {
    @tailrec
    def factorialAcc(acc: Int, n: Int): Int = {
      if (n < 1) acc
      else factorialAcc(acc * n, n - 1)
    }

    factorialAcc(1, n)
  }

  def getClassAsString(n: Any): String = n match {
    case s: String => "String"
    case f: Float => "Float"
    case i: Int => "Int"
    case _ => "Unknown"
  }

}
