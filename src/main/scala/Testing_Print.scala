
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame



object Testing_Print {

  def main(args: Array[String]){

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark=SparkSession.builder.master("local").getOrCreate

    val Lines=spark.sql("select _c0 as employee_number,_c1 as salary from (select _c0,_c1, dense_rank() over (partition by _c0 order by _c1 desc) as dn from csv.`src/main/Resource/load_salaries1.csv`) MaxSal where dn=1 order by _c0")

    Lines.show(2)

  }
}

