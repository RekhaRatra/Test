import org.apache.spark.sql._
import org.apache.log4j._



object EmpMaxSalary {

  def main(args: Array[String]): Unit = {

    //val file = args(0)

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder.master("local").getOrCreate

    val df = spark.read.option("header", "true").csv("src/main/resources/load_salaries1.csv", "src/main/resources/load_salaries2.csv", "src/main/resources/load_salaries3.csv")

    df.createOrReplaceTempView("EmployeeSal")

    spark.sql("select distinct emp_no,salary  from (SELECT *, DENSE_RANK() OVER (PARTITION BY Emp_No ORDER BY Salary desc) AS rank FROM EmployeeSal ) empsal where rank=1").repartition(1).write.format("csv").option("header","true").mode("overwrite").save("file:/Users/rekha/Documents/ratra/Sample Projects/Test/src/main/resources/EmpMaxSalary.csv")


  }

}
