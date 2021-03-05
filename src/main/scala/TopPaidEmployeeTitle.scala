
import org.apache.spark.sql._
import org.apache.log4j._



object TopPaidEmployeeTitle {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark=SparkSession.builder.master("local").getOrCreate

    val df=spark.read.option("header","true").csv("src/main/resources/load_salaries1.csv", "src/main/resources/load_salaries2.csv","src/main/resources/load_salaries3.csv")
    val title=spark.read.option("header","true").csv("src/main/resources/load_titles.csv")

    df.createOrReplaceTempView("EmployeeSal")
    title.createOrReplaceTempView(viewName = "Title")

    spark.sql("select emp_no,title from (select emp_no,title,DENSE_RANK() OVER (PARTITION BY Emp_No ORDER BY to_date desc) as dn from Title where emp_no in (select  emp_no from (SELECT *, RANK() OVER (PARTITION BY Emp_No ORDER BY Salary desc) AS rank FROM EmployeeSal )  where rank=1 limit 10)) where dn=1 ").repartition(1).write.format("csv").option("header","true").mode("overwrite").save("src/main/resources/TopPaidEmployeeTitle.csv")

  }

}
