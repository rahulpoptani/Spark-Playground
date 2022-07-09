package learnSpark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{array, array_contains, col, explode, lit, map_concat, map_keys, map_values, split, map}
import org.apache.spark.sql.types.{ArrayType, DataTypes, IntegerType, MapType, StringType, StructType}
import org.apache.spark.sql.{Column, Row, SparkSession}

import scala.collection.mutable

object ArrayTypesSpark {
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    import spark.implicits._

    /** Array Type */

    val arrayStructureData = Seq(
      Row("James,,Smith",List("Java","Scala","C++"),List("Spark","Java"),"OH","CA"),
      Row("Michael,Rose,",List("Spark","Java","C++"),List("Spark","Java"),"NY","NJ"),
      Row("Robert,,Williams",List("CSharp","VB"),List("Spark","Python"),"UT","NV")
    )
    val arrayStructureSchema = new StructType()
      .add("name",StringType)
      .add("languagesAtSchool", ArrayType(StringType))
      .add("languagesAtWork", ArrayType(StringType))
      .add("currentState", StringType)
      .add("previousState", StringType)
    val df = spark.createDataFrame(spark.sparkContext.parallelize(arrayStructureData),arrayStructureSchema)
    df.cache()
    df.printSchema()
    df.show()

    df.select(col("name"),explode(col("languagesAtSchool"))).show(false)

    df.select(split(col("name"),",").as("nameAsArray")).show(false)

    // create new array
    df.select($"name",array($"currentState",$"previousState").as("States") ).show(false)

    // array contains
    df.select(col("name"), array_contains(col("languagesAtSchool"), "Java").as("array_contains_Java")).show(false)

    /** Map Type */

    val arrayStructureData1 = Seq(
      Row("James",List(Row("Newark","NY"),Row("Brooklyn","NY")), Map("hair"->"black","eye"->"brown"), Map("height"->"5.9")),
      Row("Michael",List(Row("SanJose","CA"),Row("Sandiago","CA")), Map("hair"->"brown","eye"->"black"),Map("height"->"6")),
      Row("Robert",List(Row("LasVegas","NV")), Map("hair"->"red","eye"->"gray"),Map("height"->"6.3")),
      Row("Maria",null,Map("hair"->"blond","eye"->"red"), Map("height"->"5.6")),
      Row("Jen",List(Row("LAX","CA"),Row("Orange","CA")), Map("white"->"black","eye"->"black"),Map("height"->"5.2"))
    )

    val mapType  = DataTypes.createMapType(StringType,StringType)

    val arrayStructureSchema1 = new StructType()
      .add("name",StringType)
      .add("addresses", ArrayType(new StructType()
        .add("city",StringType)
        .add("state",StringType)))
      .add("properties", mapType)
      .add("secondProp", MapType(StringType,StringType))

    val mapTypeDF = spark.createDataFrame(spark.sparkContext.parallelize(arrayStructureData1),arrayStructureSchema1)
    mapTypeDF.cache()
    mapTypeDF.printSchema()
    mapTypeDF.show(false)

    // get all map keys and values
    mapTypeDF.select(col("name"),map_keys(col("properties")),map_values(col("properties"))).show(false)

    // merge multiple maps together
    mapTypeDF.select(col("name"),map_concat(col("properties"),col("secondProp"))).show(false)

    // dynamically create map type from dataframe column
    val structureData = Seq(
      Row("36636","Finance",Row(3000,"USA")),
      Row("40288","Finance",Row(5000,"IND")),
      Row("42114","Sales",Row(3900,"USA")),
      Row("39192","Marketing",Row(2500,"CAN")),
      Row("34534","Sales",Row(6500,"USA"))
    )

    val structureSchema = new StructType()
      .add("id",StringType)
      .add("dept",StringType)
      .add("properties",new StructType()
        .add("salary",IntegerType)
        .add("location",StringType)
      )

    var df1 = spark.createDataFrame(spark.sparkContext.parallelize(structureData),structureSchema)
    df1.show(false)

    val index = df1.schema.fieldIndex("properties")
    val propSchema = df1.schema(index).dataType.asInstanceOf[StructType]
    val columns = mutable.LinkedHashSet[Column]()
    propSchema.fields.foreach(field => {
      columns.add(lit(field.name))
      columns.add(col("properties." + field.name))
    })
    println(columns)

    df1 = df1.withColumn("propertiesMap",map(columns.toSeq:_*))
    df1 = df1.drop("properties")
    df1.printSchema()
    df1.show(false)




  }
}
