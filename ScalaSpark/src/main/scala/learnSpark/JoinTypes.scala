package learnSpark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object JoinTypes {
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    import spark.implicits._

    val emp = Seq(
      (1,"Smith",-1,"2018","10","M",3000),
      (2,"Rose",1,"2010","20","M",4000),
      (3,"Williams",1,"2010","10","M",1000),
      (4,"Jones",2,"2005","10","F",2000),
      (5,"Brown",2,"2010","40","",-1),
      (6,"Brown",2,"2010","50","",-1))
    val empColumns = Seq("emp_id","name","superior_emp_id","year_joined","emp_dept_id","gender","salary")
    val empDF = emp.toDF(empColumns:_*).cache()
    empDF.show(false)
    val dept = Seq(("Finance",10),("Marketing",20),("Sales",30),("IT",40))
    val deptColumns = Seq("dept_name","dept_id")
    val deptDF = dept.toDF(deptColumns:_*).cache()
    deptDF.show(false)

    // inner join
    empDF.join(deptDF, empDF("emp_dept_id") ===  deptDF("dept_id"),"inner").show(false)

    // full outer join - combination of left and right join
    empDF.join(deptDF, empDF("emp_dept_id") ===  deptDF("dept_id"),"outer").show(false)
    empDF.join(deptDF, empDF("emp_dept_id") ===  deptDF("dept_id"),"full").show(false)
    empDF.join(deptDF, empDF("emp_dept_id") ===  deptDF("dept_id"),"fullouter").show(false)
    empDF.join(deptDF, empDF("emp_dept_id") ===  deptDF("dept_id"),"full_outer").show(false)

    // left and right outer join
    println("Left and Right outer join")
    empDF.join(deptDF, empDF("emp_dept_id") ===  deptDF("dept_id"),"left").show(false)
    empDF.join(deptDF, empDF("emp_dept_id") ===  deptDF("dept_id"),"leftouter").show(false)
    empDF.join(deptDF, empDF("emp_dept_id") ===  deptDF("dept_id"),"right").show(false)
    empDF.join(deptDF, empDF("emp_dept_id") ===  deptDF("dept_id"),"rightouter").show(false)

    // left semi join is similar to inner join, only difference is it will skip all columns from right dataframe
    println("Left Semi Join")
    empDF.join(deptDF, empDF("emp_dept_id") ===  deptDF("dept_id"),"leftsemi").show(false)

    // left anti join will show all columns from left dataframe, but select on records which doesn't matched
    println("Left Anti Join")
    empDF.join(deptDF, empDF("emp_dept_id") ===  deptDF("dept_id"),"leftanti").show(false)

    println("Self Join")
    empDF.as("emp1").join(empDF.as("emp2"),
      col("emp1.superior_emp_id") === col("emp2.emp_id"),"inner")
      .select(
        col("emp1.emp_id"),
        col("emp1.name"),
        col("emp2.emp_id").as("superior_emp_id"),
        col("emp2.name").as("superior_emp_name"))
      .show(false)





  }
}
