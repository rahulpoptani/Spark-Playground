package learnSpark

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object WindowExpression {
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    import spark.implicits._

    val data = Seq((1, 2, "2016-03-01", 5),(1, 2, "2016-05-02", 6),(1, 3, "2017-06-25", 1),(3, 1, "2016-03-02", 0),(3, 4, "2018-07-03", 5))
    val df = data.toDF("player_id","device_id","event_date","games_played")
      .withColumn("event_date", to_date(col("event_date"), "yyyy-MM-dd"))
    df.show(false)

    // Window Function - Find Games Played so far by each player
    val windowSpec = Window.partitionBy(col("player_id")).orderBy(col("event_date"))
    val cumulativeSum = sum(col("games_played")).over(windowSpec).as("games_played_sofar")
    df.select(col("player_id"), cumulativeSum).show(false)
  }
}
