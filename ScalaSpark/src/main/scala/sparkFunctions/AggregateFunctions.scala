package sparkFunctions

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AggregateFunctions {
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    import spark.implicits._

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
      ("Saif", "Sales", 4100)
    )
    val df = simpleData.toDF("employee_name", "department", "salary")
    df.cache()
    df.show()

    println("approx_count_distinct: "+ df.select(approx_count_distinct("salary")).collect()(0)(0))

    println("avg: "+ df.select(avg("salary")).collect()(0)(0))

    df.select(collect_list("salary")).show(false)

    df.select(collect_set("salary")).show(false)


    val df2 = df.select(countDistinct("department", "salary"))
    df2.show(false)
    println("Distinct Count of Department & Salary: " + df2.collect()(0)(0))

    println("count: "+ df.select(count("salary")).collect()(0))

    df.select(
      first("salary"),last("salary"),max("salary"),min("salary"),
      mean("salary"),sum("salary"),sumDistinct("salary")).show(false)



  }
}
