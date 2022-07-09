package sparkFunctions

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json, get_json_object, json_tuple, lit, schema_of_json, to_json}
import org.apache.spark.sql.types.{MapType, StringType}

object JsonFunctions {
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    import spark.implicits._

    val jsonString="""{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"}"""
    val data = Seq((1, jsonString))
    val df = data.toDF("id","value")
    df.show(false)

    val df2 = df.withColumn("value", from_json(col("value"),MapType(StringType,StringType)))
    df2.printSchema()
    df2.show(false)

    df2.withColumn("value",to_json(col("value"))).show(false)

    df.select(col("id"),json_tuple(col("value"),"Zipcode","ZipCodeType","City"))
      .toDF("id","Zipcode","ZipCodeType","City").show(false)

    df.select(col("id"),get_json_object(col("value"),"$.ZipCodeType").as("ZipCodeType")).show(false)

    val schemaStr = spark.range(1)
      .select(schema_of_json(lit("""{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"}""")))
      .collect()(0)(0)
    println(schemaStr)



  }
}
