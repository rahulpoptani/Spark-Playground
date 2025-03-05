package learnSpark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, split, explode, monotonically_increasing_id, row_number}

object TextFileHandling {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("MySparkApp").master("local[*]").getOrCreate()
    val df = spark.read.option("skipRows", 1).text("ScalaSpark/src/main/resources/data/word_count.txt").toDF("words")

    // word count
    df.select(explode(split(col("words"), " ")).as("exploded")).groupBy(col("exploded")).count().show()

    // skip first row
    df.withColumn("row_number", row_number().over(Window.orderBy(monotonically_increasing_id()))).filter(col("row_number").gt(1)).show()
  }
}
