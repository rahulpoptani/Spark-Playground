package sparkFunctions

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DateFunctions {
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    import spark.implicits._


    Seq(("2019-01-23")).toDF("Input")
      .select(
        current_date()as("current_date"),
        col("Input"),
        date_format(col("Input"), "MM-dd-yyyy").as("format")
      ).show(false)


    Seq(("04/13/2019")).toDF("Input")
      .select( col("Input"),
        to_date(col("Input"), "MM/dd/yyyy").as("to_date")
      ).show()


    Seq(("2019-01-23"),("2019-06-24"),("2019-09-20")).toDF("date")
      .select( col("date"), current_date(),
        datediff(current_date(),col("date")).as("datediff"),
        months_between(current_date(),col("date")).as("months_between")
      ).show()


    Seq(("2019-01-23"),("2019-06-24"),("2019-09-20")).toDF("input")
      .select( col("input"),
        trunc(col("input"),"Month").as("Month_Trunc"),
        trunc(col("input"),"Year").as("Month_Year")
      ).show()


    Seq(("2019-01-23"),("2019-06-24"),("2019-09-20")).toDF("input")
      .select( col("input"),
        add_months(col("input"),3).as("add_months"),
        add_months(col("input"),-3).as("sub_months"),
        date_add(col("input"),4).as("date_add"),
        date_sub(col("input"),4).as("date_sub")
      ).show()


    Seq(("2019-01-23"),("2019-06-24"),("2019-09-20")).toDF("input")
      .select( col("input"),
        year(col("input")).as("year"),
        month(col("input")).as("month"),
        dayofweek(col("input")).as("dayofweek"),
        dayofmonth(col("input")).as("dayofmonth"),
        dayofyear(col("input")).as("dayofyear"),
        next_day(col("input"),"Sunday").as("next_day"),
        weekofyear(col("input")).as("weekofyear")
      ).show()


    val df = Seq((1)).toDF("seq")
    val curDate = df.withColumn("current_date",current_date().as("current_date"))
      .withColumn("current_timestamp",current_timestamp().as("current_timestamp"))
    curDate.show(false)


    val dfDate = Seq(("07-01-2019 12 01 19 406"),
      ("06-24-2019 12 01 19 406"),
      ("11-16-2019 16 44 55 406"),
      ("11-16-2019 16 50 59 406")).toDF("input_timestamp")
    dfDate.withColumn("datetype_timestamp",
      to_timestamp(col("input_timestamp"),"MM-dd-yyyy HH mm ss SSS"))
      .show(false)


    val df1 = Seq(("2019-07-01 12:01:19.000"),
      ("2019-06-24 12:01:19.000"),
      ("2019-11-16 16:44:55.406"),
      ("2019-11-16 16:50:59.406")).toDF("input_timestamp")
    df1.withColumn("hour", hour(col("input_timestamp")))
      .withColumn("minute", minute(col("input_timestamp")))
      .withColumn("second", second(col("input_timestamp")))
      .show(false)












  }
}
