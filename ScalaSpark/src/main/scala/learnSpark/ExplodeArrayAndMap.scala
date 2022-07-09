package learnSpark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{explode, explode_outer, posexplode, posexplode_outer}
import org.apache.spark.sql.types.{ArrayType, MapType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object ExplodeArrayAndMap {
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    import spark.implicits._

    val arrayData = Seq(
      Row("James",List("Java","Scala"),Map("hair"->"black","eye"->"brown")),
      Row("Michael",List("Spark","Java",null),Map("hair"->"brown","eye"->null)),
      Row("Robert",List("CSharp",""),Map("hair"->"red","eye"->"")),
      Row("Washington",null,null),
      Row("Jefferson",List(),Map())
    )

    val arraySchema = new StructType()
      .add("name",StringType)
      .add("knownLanguages", ArrayType(StringType))
      .add("properties", MapType(StringType,StringType))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(arrayData),arraySchema)
    df.cache()
    df.printSchema()
    df.show(false)

    // explode array
    df.select($"name",explode($"knownLanguages")).show(false)

    // explode map
    df.select($"name",explode($"properties")).show(false)

    // explode outer - create row for each element
    df.select($"name",explode_outer($"knownLanguages")).show(false)
    df.select($"name",explode_outer($"properties")).show(false)

    // posexplode is similar to explode - creates additional column specifying the position
    df.select($"name",posexplode($"knownLanguages")).show(false)
    df.select($"name",posexplode($"properties")).show(false)

    // posexplode oyter - similar to posexplode - creates one row for each element
    df.select($"name",posexplode_outer($"knownLanguages")).show(false)
    df.select($"name",posexplode_outer($"properties")).show(false)









  }
}
