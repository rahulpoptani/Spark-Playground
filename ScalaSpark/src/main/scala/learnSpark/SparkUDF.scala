package learnSpark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, initcap, udf}

object SparkUDF {
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    import spark.implicits._

    val columns = Seq("Seqno","Quote")
    val data = Seq(
      ("1", "Be the change that you wish to see in the world"),
      ("2", "Everyone thinks of changing the world, but no one thinks of changing himself."),
      ("3", "The purpose of our lives is to be happy."))
    val df = data.toDF(columns:_*)
    df.show(false)

    // convert first character of each word to uppercase
    val convertCase = (str:String) => {
      val arr = str.split(" ")
      arr.map(f => f.substring(0,1).toUpperCase + f.substring(1,f.length)).mkString(" ")
    }
    val convertUDF = udf(convertCase)
    df.select(col("Seqno"), convertUDF(col("Quote")).as("Quote")).show(false)
    // Using it on SQL
    spark.udf.register("convertUDF", convertCase)
    df.createOrReplaceTempView("QUOTE_TABLE")
    spark.sql("select Seqno, convertUDF(Quote) as Quote from QUOTE_TABLE").show(false)

    // UDF are black box, and you will lose all the optimization what spark could have done

    // Can be Done Using DataFrame Methods
    df.withColumn("Quote", initcap(col("Quote"))).show(false)


  }
}
