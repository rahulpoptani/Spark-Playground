package learnSpark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{StructType, StringType, IntegerType}
import org.apache.spark.sql.{Row, SparkSession}

object MapAndMapPartitions {
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    import spark.implicits._

    val structureData = Seq(
      Row("James","","Smith","36636","NewYork",3100),
      Row("Michael","Rose","","40288","California",4300),
      Row("Robert","","Williams","42114","Florida",1400),
      Row("Maria","Anne","Jones","39192","Florida",5500),
      Row("Jen","Mary","Brown","34561","NewYork",3000)
    )

    val structureSchema = new StructType()
      .add("firstname",StringType)
      .add("middlename",StringType)
      .add("lastname",StringType)
      .add("id",StringType)
      .add("location",StringType)
      .add("salary",IntegerType)

    val df2 = spark.createDataFrame(spark.sparkContext.parallelize(structureData),structureSchema)
    df2.show(false)

    val df3 = df2.map(row => {
      // initialize foreach record, not good for heavy initialization like database
      val util = new Util()
      val fullName = util.combine(row.getString(0),row.getString(1),row.getString(2))
      (fullName, row.getString(3),row.getInt(5))
    })
    val df3Map =  df3.toDF("fullName","id","salary")
    df3Map.show(false)

    // mapPartitions allows heavy initialization like database connect, it does for each partitions rather than each record like map
    val df4 = df2.mapPartitions(iterator => {
      val util = new Util() // heavy initialization goes here..
      val res = iterator.map(row => {
        val fullName = util.combine(row.getString(0),row.getString(1),row.getString(2))
        (fullName, row.getString(3),row.getInt(5))
      })
      res // return iterator
    })
    val df4part = df4.toDF("fullName","id","salary")
    df4part.show(false)

  }
}


class Util extends Serializable {
  def combine(fname:String, mname:String, lname:String): String = {
    fname + "," + mname + "," + lname
  }
}
