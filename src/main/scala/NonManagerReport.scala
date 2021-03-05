
import org.apache.spark.sql._
import org.apache.log4j._



object NonManagerReport {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark=SparkSession.builder.master("local").getOrCreate

    val emp=spark.read.option("header","true").csv("src/main/resources/load_employees.csv")
    val mag=spark.read.option("header","true").csv("src/main/resources/load_dept_manager.csv")
    emp.createOrReplaceTempView("Employee")
    mag.createOrReplaceTempView(viewName = "Manager")

    spark.sql("select distinct emp_no from Employee where emp_no not in (select emp_no from Manager)").repartition(1).write.format("csv").option("header","true").mode("overwrite").save("src/main/resources/NonManagerEmployee.csv")

  }

}
