package sparkFunctions

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object WindowFunctions {
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    import spark.implicits._

    val simpleData = Seq(("James", "Sales", 3000),
      ("Michael", "Sales", 4600),
      ("Robert", "Sales", 4100),
      ("Maria", "Finance", 3000),
      ("James", "Sales", 3000),
      ("Scott", "Finance", 3300),
      ("Jen", "Finance", 3900),
      ("Jeff", "Marketing", 3000),
      ("Kumar", "Marketing", 2000),
      ("Saif", "Sales", 4100)
    )
    val df = simpleData.toDF("employee_name", "department", "salary")
    df.show()

    val windowSpec  = Window.partitionBy("department").orderBy("salary")
    df.withColumn("row_number",row_number.over(windowSpec))
      .withColumn("rank",rank().over(windowSpec))
      .withColumn("dense_rank",dense_rank().over(windowSpec))
      .withColumn("percent_rank",percent_rank().over(windowSpec))
      .withColumn("ntile",ntile(2).over(windowSpec))
      .withColumn("cume_dist",cume_dist().over(windowSpec))
      .withColumn("lag",lag("salary",2).over(windowSpec))
      .withColumn("lead",lead("salary",2).over(windowSpec))
      .show(false)

    val windowSpecAgg  = Window.partitionBy("department")
    df.withColumn("row",row_number.over(windowSpec))
      .withColumn("avg", avg(col("salary")).over(windowSpecAgg))
      .withColumn("sum", sum(col("salary")).over(windowSpecAgg))
      .withColumn("min", min(col("salary")).over(windowSpecAgg))
      .withColumn("max", max(col("salary")).over(windowSpecAgg))
      .where(col("row")===1).select("department","avg","sum","min","max")
      .show()



  }
}
