package part4sql

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object SparkSql extends App {

  val spark=SparkSession.builder()
    .appName("spark sql practice")
    .config("spark.master","local")
    .config("spark.sql.warehouse.dir","src/main/resources/warehouse")
   // .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
    .getOrCreate()

  val carsDF=spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/cars.json")

  //regular DF API
  carsDF.select(col("Name")).where(col("Origin") === "USA")

  //use spark SQL
  carsDF.createOrReplaceTempView("cars")
  val americanCarsDF=spark.sql(
    """
      |select Name from cars where Origin ='USA'
      |""".stripMargin)

  //we can run any sql statements
  spark.sql("create database rtjvm")
  spark.sql("use rtjvm")
val databasesDF=spark.sql("show databases")
databasesDF.show()

  //how to transform tables from a db to spark tables
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

  def transferTables(tableNames:List[String],shouldWriteToWarehouse:Boolean=false)=tableNames.foreach{tableName=>
    val tableDF=readTable(tableName)
    tableDF.createOrReplaceTempView(tableName)
    if(shouldWriteToWarehouse) {
      tableDF.write
        .mode(SaveMode.Overwrite)
        .saveAsTable(tableName)
    }
  }
 /* val employeesDF=readTable("employees")
  employeesDF.write
    .mode(SaveMode.Overwrite)
    .saveAsTable("employees")
*/

  transferTables(List(
    "employees",
    "departments",
    "dept_manager",
    "titles",
    "dept_emp",
    "salaries")
  )
  //read df from loaded spark tables
 val employeeDF2=spark.read.table("employees")

  /**
    * Exercises
    * 1.read the movies Df and store it as a spark table in the rtjvm
    * 2.count how many employees were hired in between january 1 1999 and jan 1 2000
    *3.show the avg salary for the employees hired between those dates,grouped by departments
    * 4.show the name of the best -paying department for employees  hired in betwenn those dates.
    */
  /*spark.sql(
    """
      |select *
      |from employees e,dept_emp d
      |where e.emp_no = d.emp_no
      |""".stripMargin
  )*/

  //1.
  val moviesDF=spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/movies.json")

  /*moviesDF.write
    .mode(SaveMode.Overwrite)
    .saveAsTable("movies")
  */

  //2.
  spark.sql(
    """
      |select count(*)
      |from employees
      |where hire_date > '1999-01-01' and hire_date < '2000-01-01'
      |""".stripMargin
  )

  //3.
  spark.sql(
    """
      |select de.dept_no,avg(s.salary)
      |from salaries s,dept_emp de,employees e
      |where e.hire_date > '1999-01-01' and e.hire_date < '2000-01-01'
      | and e.emp_no = de.emp_no
      | and e.emp_no=s.emp_no
      | group by de.dept_no
      |""".stripMargin
  )

  //4.
  spark.sql(
    """
      |select avg(s.salary) payments,d.dept_name
      |from salaries s,dept_emp de,employees e,departments d
      |where e.hire_date > '1999-01-01' and e.hire_date < '2000-01-01'
      |and e.emp_no=de.emp_no
      |and e.emp_no=s.emp_no
      |and de.dept_no=d.dept_no
      |group by d.dept_name
      |order by payments desc
      |limit 1
      |""".stripMargin
  ).show()

}
