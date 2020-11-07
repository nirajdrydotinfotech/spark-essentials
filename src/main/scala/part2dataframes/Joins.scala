package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, first, max}

object Joins extends App{

  val spark=SparkSession.builder()
    .appName("Joins")
    .config("spark.master","local")
    .getOrCreate()

  val guitarsDF=spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/guitars.json")

  val guitaristDF=spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/guitarPlayers.json")

  val bandDF=spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/bands.json")

  //inner-joins
  val joinCondition= guitaristDF.col("band")===bandDF.col("id")
  val guitaristBandDF=guitaristDF.join(bandDF, joinCondition, "inner")
guitaristBandDF.show()

  //outer joins
  //left outer=everything in the inner join+all the rows in the left table,with the null where the data is missing
  guitaristDF.join(bandDF,joinCondition,"left_outer")

  //right outer=everything in the inner join+all the rows in the right table,with the null where the data is missing
  guitaristDF.join(bandDF,joinCondition,"right_outer")

  //full outer=everything in the inner join+all the rows in the both table,with the null where the data is missing
guitaristDF.join(bandDF,joinCondition,"outer")

  //semi-join=everything in the left df for which there is a row in the right df satisfying the condition
  guitaristDF.join(bandDF,joinCondition,"left_semi")

  //anti-join=everything in the left df for which there is no row in the right df satisfying the condition
  guitaristDF.join(bandDF,joinCondition,"left_anti").show()

  //things to bear in the mind
  //1.guitaristBandDF.select("id","band")  //this crashes id is ambigious which id to choose

    //option 1-rename the column on which we are joining
  guitaristDF.join(bandDF.withColumnRenamed("id","band"),"band")

  //option2-drop the duplicate drop
  guitaristBandDF.drop(bandDF.col("id"))

  //option3-rename the offending column
  val bandsModDF=bandDF.withColumnRenamed("id","bandId")
  guitaristDF.join(bandsModDF,guitaristDF.col("band")===bandsModDF.col("bandId"))

//2.using complex types
  /*guitaristDF.join(guitarsDF.withColumnRenamed("id","guitarId"),
    expr("array_contains(guitars, guitarsId"))
*/
  /**
    * Exercises
    * 1.show all employees and their max salary
    * 2.show all employee who are never managers
    * 3.find job title of the best paid 10 employees in the company
    */
  val driver="org.postgresql.Driver"
  val url="jdbc:postgresql://localhost:5432/rtjvm"
  val user="docker"
  val password="docker"
    def readTable(tableName:String)=spark.read
      .format("jdbc")
      .option("driver",driver)
      .option("url",url)
      .option("user",user)
      .option("password",password)
      .option("dbtable",s"public.${tableName}")
      .load()

  val salariesDF=readTable("salaries")
  val employeesDF=readTable("employees")
  val deptMangerDF=readTable("dept_manager")
  val titleDF=readTable("titles")

  //1
  val joinCondition1= salariesDF.groupBy("emp_no").agg(max("salary").as("maxSalary"))
 val employeeSalaryDF= employeesDF.join(joinCondition1,"emp_no")

  //2
  employeesDF.join(deptMangerDF,
    employeesDF.col("emp_no") === deptMangerDF.col("emp_no"),
    "left_anti").show()

  //3
  val mostRecentJobTitle=titleDF.groupBy("emp_no").agg(max("to_date"))
  val bestPaidEmployeesDF=employeeSalaryDF.orderBy(col("maxSalary").desc).limit(10)
  val bestPaidJobs=bestPaidEmployeesDF.join(mostRecentJobTitle,"emp_no")
  bestPaidJobs.show()


}
