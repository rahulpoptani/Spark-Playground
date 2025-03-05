package learnSpark

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Logger, Level}
import org.apache.spark.sql.functions._

object Dates {
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    // Current Day and Time
    val dateDF = spark.range(1).withColumn("today", current_date()).withColumn("now",current_timestamp)

    // Add and Subtract Days
    dateDF
      .select(col("today"), col("now"),
        date_add(col("today"), 5).as("5DaysAhead"),
        date_sub(col("today"), 5).as("5DaysBack"))
      .show(false)

    // Months Between two days
    spark.range(1)
      .select(
        to_date(lit("2018-01-01"), "yyyy-MM-dd").alias("start_date"),
        to_date(lit("2018-10-01"), "yyyy-MM-dd").alias("end_date"))
      .select(months_between(col("end_date"), col("start_date")).as("months_between"))
      .show(false)
  }
}
