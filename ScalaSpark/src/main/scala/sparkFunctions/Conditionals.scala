package sparkFunctions

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, when}

object Conditionals {
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    import spark.implicits._

    val data = List(("James", "", "Smith", "36636", "M", 60000),
      ("Michael", "Rose", "", "40288", "M", 70000),
      ("Robert", "", "Williams", "42114", "", 400000),
      ("Maria", "Anne", "Jones", "39192", "F", 500000),
      ("Jen", "Mary", "Brown", "", "F", 0))
    val cols = Seq("first_name", "middle_name", "last_name", "dob", "gender", "salary")
    val df = spark.createDataFrame(data).toDF(cols: _*)

    // Using WHEN to derived new fields
    val df2 = df.withColumn("new_gender1",
      when(col("gender").equalTo("M"), "Male")
        .when(col("gender").equalTo("F"), "Female")
        .otherwise("Unknown"))
    df2.show(false)

    val df3 = df.withColumn("new_gender2",
      expr("case when gender = 'M' then 'Male' when gender = 'F' then 'Female' else 'Unknown' end"))
    df3.show(false)


    val dataDF = Seq(
      (66, "a", "4"), (67, "a", "0"), (70, "b", "4"), (71, "d", "4"
      )).toDF("id", "code", "amt")
    dataDF.withColumn("new_column",
      when(col("code") === "a" || col("code") === "d", "A")
        .when(col("code") === "b" && col("amt") === "4", "B")
        .otherwise("A1"))
      .show()


  }
}
