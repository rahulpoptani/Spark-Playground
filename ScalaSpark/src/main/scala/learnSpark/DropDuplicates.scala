package learnSpark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object DropDuplicates {
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    val simpleData = Seq(
      ("James", "Sales", 3000),
      ("Michael", "Sales", 4600),
      ("Robert", "Sales", 4100),
      ("Maria", "Finance", 3000),
      ("James", "Sales", 3000),
      ("Scott", "Finance", 3300),
      ("Jen", "Finance", 3900),
      ("Jeff", "Marketing", 3000),
      ("Kumar", "Marketing", 2000),
      ("Saif", "Sales", 4100))

    val df = spark.createDataFrame(simpleData).toDF("employee_name", "department", "salary")
    println("Total count: " + df.count())
    df.show()

    val distinctDF = df.distinct()
    println("Distinct count: " + distinctDF.count())
    distinctDF.show(false)

    val df2 = df.dropDuplicates()
    println("Distinct count: " + df2.count())
    df2.show(false)

    val dropDisDF = df.dropDuplicates("department","salary")
    println("Distinct count of department & salary : " + dropDisDF.count())
    dropDisDF.show(false)

  }
}
