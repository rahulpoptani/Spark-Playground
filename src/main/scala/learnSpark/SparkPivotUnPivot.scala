package learnSpark

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{DataType, StructType}

object SparkPivotUnPivot {
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Test").master("local[*]").getOrCreate()
    import spark.implicits._

    val data = Seq((1,100, "Jan"),(1,200, "Jan"),(1,200, "Feb"),(2,110, "Feb"),(3,120, "Mar"))
    val df = data.toDF("id","sale","month")
    df.show(false)

    df
      .groupBy("id")
      .pivot("month",List("Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"))
      .sum("sale")
      .orderBy("id")
      .show(false)

    val data1 = Seq(("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"),
      ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"),
      ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"),
      ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico"))

    val df1 = data1.toDF("Product","Amount","Country")
    df1.show()

    val pivotDF = df1.groupBy("Product").pivot("Country").sum("Amount") // for performance improvements provide pivot column list beforehand
    pivotDF.show()


  }
}
