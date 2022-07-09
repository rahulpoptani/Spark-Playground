package learnSpark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, flatten}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.{Column, Row, SparkSession}

object FlattenNestedStruct {
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    import spark.implicits._
    val structureData = Seq(
      Row(Row("James ","","Smith"),Row(Row("CA","Los Angles"),Row("CA","Sandiago"))),
      Row(Row("Michael ","Rose",""),Row(Row("NY","New York"),Row("NJ","Newark"))),
      Row(Row("Robert ","","Williams"),Row(Row("DE","Newark"),Row("CA","Las Vegas"))),
      Row(Row("Maria ","Anne","Jones"),Row(Row("PA","Harrisburg"),Row("CA","Sandiago"))),
      Row(Row("Jen","Mary","Brown"),Row(Row("CA","Los Angles"),Row("NJ","Newark")))
    )

    val structureSchema = new StructType()
      .add("name",new StructType()
        .add("firstname",StringType)
        .add("middlename",StringType)
        .add("lastname",StringType))
      .add("address",new StructType()
        .add("current",new StructType()
          .add("state",StringType)
          .add("city",StringType))
        .add("previous",new StructType()
          .add("state",StringType)
          .add("city",StringType)))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(structureData),structureSchema)
    df.cache()
    df.printSchema()
    df.show(false)

    val df2 = df.select(col("name.*"), col("address.current.*"), col("address.previous.*"))
    df2.toDF("fname","mename","lname","currAddState", "currAddCity","prevAddState","prevAddCity").show(false)


    def flattenStructSchema(schema: StructType, prefix: String = null) : Array[Column] = {
      schema.fields.flatMap(f => {
        val columnName = if (prefix == null) f.name else (prefix + "." + f.name)

        f.dataType match {
          case st: StructType => flattenStructSchema(st, columnName)
          case _ => Array(col(columnName).as(columnName.replace(".","_")))
        }
      })
    }

    val df3 = df.select(flattenStructSchema(df.schema):_*)
    df3.printSchema()
    df3.show(false)

    // Flatten nested array
    val arrayArrayData = Seq(
      Row("James",List(List("Java","Scala","C++"),List("Spark","Java"))),
      Row("Michael",List(List("Spark","Java","C++"),List("Spark","Java"))),
      Row("Robert",List(List("CSharp","VB"),List("Spark","Python")))
    )

    val arrayArraySchema = new StructType()
      .add("name",StringType)
      .add("subjects",ArrayType(ArrayType(StringType)))

    val df4 = spark.createDataFrame(spark.sparkContext.parallelize(arrayArrayData),arrayArraySchema)
    df4.printSchema()
    df4.show(false)

    // flatten creates single array from array of arrays
    df4.select($"name",flatten($"subjects")).show(false)






  }
}
