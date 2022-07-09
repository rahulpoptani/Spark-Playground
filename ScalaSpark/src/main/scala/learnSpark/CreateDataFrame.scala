package learnSpark

import org.apache.spark.sql.{Encoders, Row, SaveMode, SparkSession}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.parser.SqlBaseParser.CreateHiveTableContext
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.hive.{HiveContext, HiveUDAFBuffer}
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructField, StructType}


object CreateDataFrame {
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getName).enableHiveSupport().master("local").getOrCreate()
    import spark.implicits._

    // Sequence to DF
    val one = Seq((1, "ABC"),(2, "DEF"))
    val oneDF = one.toDF("id", "name")
    oneDF.show(false)

    // Sequence to RDD to DF with Schema
    val two = Seq(Row(1, "ABC"), Row(2, "DEF"))
    val twoRDD = spark.sparkContext.parallelize(two)
    val schema = StructType(List(StructField("id", IntegerType),StructField("name", StringType)))
    val twoDF = spark.createDataFrame(twoRDD, schema)
    twoDF.show(false)

    // Store dataframe as hive table
    twoDF.write.mode(SaveMode.Overwrite).saveAsTable("twoDF")
    // Read table from Hive Context
    val hiveContext = new HiveContext(spark.sparkContext)
    val hiveDF = hiveContext.sql("select * from twoDF")
    hiveDF.show(false)

    // Create DataFrame from List
    val four = Seq((1, "ABC"),(2, "DEF"))
    val fourCols = List("id", "name")
    val fourDF = spark.createDataFrame(four).toDF(fourCols:_*)
    fourDF.show(false)

    // Select DF Columns from List
    val cols = List("id","name")
    fourDF.select(cols.map(x => col(x)):_*).show(false)

    // Split columns into multiple columns
    val columns = Seq("name","address")
    val data = Seq(("Robert, Smith", "1 Main st, Newark, NJ, 92537"),("Maria, Garcia","3456 Walnut st, Newark, NJ, 94732"))
    val dfFromData = spark.createDataFrame(data).toDF(columns:_*)
    val newDF = dfFromData.map(row => {
      val nameSplit = row.getAs[String](0).split(",")
      val addSplit = row.getAs[String](1).split(",")
      (nameSplit(0),nameSplit(1),addSplit(0),addSplit(1),addSplit(2),addSplit(3))
    })
    val finalDF = newDF.toDF("First Name","Last Name","Address Line1","City","State","zipCode")
    finalDF.printSchema()
    finalDF.show(false)

    // dynamically rename multiple columns
    val old_columns = Seq("dob","gender","salary","fname","mname","lname")
    val new_columns = Seq("DateOfBirth","Sex","salary","firstName","middleName","lastName")
    val columnsList = old_columns.zip(new_columns).map(f=>{col(f._1).as(f._2)})
    // df.select(columnsList:_*)

    // Storing prettyJson schema as string or using string schema while DF creation
    val structureData = Seq(
      Row(Row("James ","","Smith"),"36636","M",3100),
      Row(Row("Michael ","Rose",""),"40288","M",4300),
      Row(Row("Robert ","","Williams"),"42114","M",1400),
      Row(Row("Maria ","Anne","Jones"),"39192","F",5500),
      Row(Row("Jen","Mary","Brown"),"","F",-1))
    val structureSchema = new StructType()
      .add("name",new StructType()
        .add("firstname",StringType)
        .add("middlename",StringType)
        .add("lastname",StringType))
      .add("id",StringType)
      .add("gender",StringType)
      .add("salary",IntegerType)
    val structureDataDF = spark.createDataFrame(spark.sparkContext.parallelize(structureData),structureSchema)
    val stringSchema = structureDataDF.schema.prettyJson
    val schemaFromJson = DataType.fromJson(stringSchema).asInstanceOf[StructType]
    val externalSchemaDF = spark.createDataFrame(spark.sparkContext.parallelize(structureData),schemaFromJson)




  }
}
