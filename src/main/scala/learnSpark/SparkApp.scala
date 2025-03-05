package learnSpark

import org.apache.spark.sql.SparkSession

object SparkApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("MySparkApp")
      .master("spark://localhost:7077")
      .config("spark.driver.host", "localhost")
      .getOrCreate()


    // Your Spark logic here
    println("Spark application started successfully!")
    spark.stop()
  }
}