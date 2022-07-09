package learnSpark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkBroadcast {
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()

    val states = Map(("NY","New York"),("CA","California"),("FL","Florida"),("DL","Delhi"))
    val countries = Map(("USA","United States of America"),("IN","India"))

    val broadcastStates = spark.sparkContext.broadcast(states)
    val broadcastCountries = spark.sparkContext.broadcast(countries)

    val data = Seq(
      ("James","Smith","USA","CA"),
      ("Michael","Rose","USA","NY"),
      ("Robert","Williams","USA","CA"),
      ("Bablu","Pandey","IN","DL"))

    val columns = Seq("firstname","lastname","country","state")
    import spark.implicits._
    val df = data.toDF(columns:_*)

    val df2 = df.map(row => {
      val country = row.getString(2)
      val state = row.getString(3)

      val fullCountry = broadcastCountries.value(country)
      val fullState = broadcastStates.value(state)
      (row.getString(0),row.getString(1),fullCountry,fullState)
    }).toDF(columns:_*)

    df2.show(false)


  }
}
