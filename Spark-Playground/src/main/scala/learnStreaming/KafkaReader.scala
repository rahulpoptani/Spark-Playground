package learnStreaming

import org.apache.spark.sql.SparkSession

object KafkaReader {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("kafkaReader").master("local[*]").getOrCreate()
    val df = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "localhost:29092")
      .option("subscribe", "test")
      .load()

    df.printSchema()

    val query = df.writeStream.outputMode("append").format("console").start()
    query.awaitTermination()

  }
}
