package learnSpark

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Logger, Level}

object Tuning {
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    val df = spark.read.option("header",true).csv("src/main/resources/all_india_index.csv")

    // Calculate size of dataframe
    val catalystPlan = df.queryExecution.logical
    val sizeInBytes = spark.sessionState.executePlan(catalystPlan).optimizedPlan.stats
    println(sizeInBytes)
  }
}
