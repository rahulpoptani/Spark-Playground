package learn

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.functions.{array_contains, col}

object FilterConditions {
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()

    val arrayStructureData = Seq(
      Row(Row("James","","Smith"),List("Java","Scala","C++"),"OH","M"),
      Row(Row("Anna","Rose",""),List("Spark","Java","C++"),"NY","F"),
      Row(Row("Julia","","Williams"),List("CSharp","VB"),"OH","F"),
      Row(Row("Maria","Anne","Jones"),List("CSharp","VB"),"NY","M"),
      Row(Row("Jen","Mary","Brown"),List("CSharp","VB"),"NY","M"),
      Row(Row("Mike","Mary","Williams"),List("Python","VB"),"OH","M")
    )

    val arrayStructureSchema = new StructType()
      .add("name",new StructType()
        .add("firstname",StringType)
        .add("middlename",StringType)
        .add("lastname",StringType))
      .add("languages", ArrayType(StringType))
      .add("state", StringType)
      .add("gender", StringType)

    val df = spark.createDataFrame(
              spark.sparkContext.parallelize(arrayStructureData),arrayStructureSchema)
    df.printSchema()
    df.show()

    // Filter with Column Condition
    df.filter(col("state") === "OH").show(false)

    // Filter with SQL Expression, alternatively can be used with WHERE function
    df.filter(" state = 'OH' ").show(false)

    // Multiple Filter Conditions
    df.filter(col("state") === "OH" && col("gender") === "M").show(false)

    // Filter on Array Column
    df.filter(array_contains(col("languages"), "Java")).show(false)

    // Filter on Nested Data Structures
    df.filter(col("name.lastname") === "Williams").show(false)

  }
}
