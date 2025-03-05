package learnSpark

//package main.scala.learnSpark
//
//import com.fasterxml.jackson.databind.ObjectMapper
//import com.fasterxml.jackson.module.scala.DefaultScalaModule
//import org.apache.spark.Success
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.execution.streaming.CommitMetadata.format
//import org.apache.spark.sql.functions.{col, json_tuple}
//import org.json4s.JValue
//import sttp.client4.quick._
//import sttp.client4.Response
//
//import scala.collection.mutable.ListBuffer
//
//object CallApi {
//  def main(args: Array[String]): Unit = {
//    val spark = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
//    import spark.implicits._
//    val data = Seq((1, 40.7128, -74.0060, ""), (2, 34.0522, -118.2437, "Los Angeles"), (3, 51.5074, -0.1278, "")).toDF("user_id", "latitude", "longitude", "city")
//    val result = data.mapPartitions(partition => {
//      val buffer = ListBuffer[(Int, Double, Double, String)]()
//      partition.foreach(row => {
//        val latitude = row.getAs[Double]("latitude")
//        val longitude = row.getAs[Double]("longitude")
//        var city = row.getAs[String]("city")
//        if (city.isEmpty) {
//          city = getCity(latitude, longitude)
//          val json: JValue = org.json4s.jackson.parseJson(city)
//          city = (json \ "city").extractOpt[String].getOrElse("")
//        }
//        buffer.append((row.getAs[Int]("user_id"), latitude, longitude, city))
//      })
//      buffer.iterator
//    }).toDF(data.schema.fields.map(_.name):_*)
//    result.show()
//
//    // another approach using UDF
//    val getCityUDF = spark.udf.register("getCityUDF", getCity(_: Double, _: Double))
//    val result2 = data.where(col("city").equalTo("")).withColumn("city", getCityUDF($"latitude", $"longitude"))
//      .withColumn("city", json_tuple(col("city"), "city"))
//      .union(data.where(col("city").notEqual("")))
//    result2.show()
//  }
//
//  def getCity(latitude: Double, longitude: Double): String = {
//    try {
//      val response = quickRequest.get(uri"http://localhost:8080/getCity?latitude=$latitude&longitude=$longitude").send()
//      response.body
//    }
//    catch {
//      case e: Exception =>
//        val mapper = new ObjectMapper()
//        mapper.registerModule(DefaultScalaModule)
//        mapper(Map("city" -> "Unknown")).writeValueAsString()
//    }
//  }
//
//}
//// Run Go Server which takes latitude and longitude params and return random city
//// go run basics/http/server.go