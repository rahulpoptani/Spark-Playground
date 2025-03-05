package learnSpark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Aggregates02 {
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    val df = spark.read.option("header",true).csv("src/main/resources/airbnb_session.csv")

    // top 5 device counts
    df.groupBy(col("dim_device_app_combo")).count().sort(col("count").desc).limit(5).show(false)

  }
}
