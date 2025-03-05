package learnSpark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object MapAndFlatMap {
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    import spark.implicits._

    val data = Seq(
      "Project Gutenberg’s",
      "Alice’s Adventures in Wonderland",
      "Project Gutenberg’s",
      "Adventures in Wonderland",
      "Project Gutenberg’s")
    val df = data.toDF("data")
    df.show(false)

    val mapDF = df.map(record => record.getString(0).split(" "))
    mapDF.show(false)

    val flatMapDF = df.flatMap(record => record.getString(0).split(" "))
    flatMapDF.show(false)

  }
}
