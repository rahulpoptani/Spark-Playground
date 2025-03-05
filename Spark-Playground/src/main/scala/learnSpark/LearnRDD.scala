package learnSpark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object LearnRDD {
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getName).master("local").getOrCreate()

    val rdd: RDD[Int] = spark.sparkContext.parallelize(List(1,2,3,4,5,6,7,8,9),3)
    val rddCollect: Array[Int] = rdd.collect()
    println(s"Number of partitions: ${rdd.getNumPartitions}")
    val rddRepartitioned = rdd.repartition(5)
    println(s"Number of partitions: ${rddRepartitioned.getNumPartitions}")
    println(s"Action: First Element: ${rdd.first()}")
    println(s"RDD converted to Array[Int]:")
    rddCollect.foreach(println)

    // Create Empty RDD
    type pairRDD = (String, Int)
    val emptyRDD = spark.sparkContext.emptyRDD[pairRDD]
    emptyRDD.foreach(println) // nothing will print

    // Remove Header from File
    val fileRDD: RDD[String] = spark.sparkContext.textFile("src/main/resources/all_india_index.csv")
    val removedHeaderRDD = fileRDD.mapPartitionsWithIndex((idx, itr) =>
    if (idx == 0) itr.drop(1)
    else itr
    )
    removedHeaderRDD.foreach(println)

    // Word Count
    val wordRegex = "[a-zA-Z]".r
    removedHeaderRDD
      .flatMap(x => x.split(","))
      .map(x => (x,1))
      .reduceByKey(_+_)
      .foreach(println)




  }
}
