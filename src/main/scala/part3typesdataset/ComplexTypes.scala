package part3typesdataset

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ComplexTypes extends App {

  val spark=SparkSession.builder()
    .appName("Complex Data Types")
    .config("spark.master","local")
    .getOrCreate()

  val moviesDF=spark.read.option("inferSchema","true").json("src/main/resources/data/movies.json")

  //Dates
  val moviesWithReleaseDates=moviesDF.select(col("Title"),
    to_date(col("Release_Date"),"d-MMM-yy")
      .as("Actual_release")) //conversion

    moviesWithReleaseDates
    .withColumn("Today", current_date())
    .withColumn("Right_Now",current_timestamp())
    .withColumn("Movie_Age", datediff(col("Today")
        ,col("Actual_release"))/365) //date_add , date_sub

  moviesWithReleaseDates.select("*").where(col("Release_Date").isNull).show()

  /**
    * Exercise
    * 1.how do we deal with multiple date formats?
    * 2.Read the stocks DF and parse the dates
    */

    //1-parse the df multiple times ,then union the small dfs
    //2
  val stocksDF=spark.read.option("inferSchema","true")
    .option("header","true")
    .csv("src/main/resources/data/stocks.csv")
  val dateParserStocks=stocksDF.withColumn("actual_date",
    to_date(col("date"),"MMM d yyyy")).show()

  //structures
 //1.with col operators
  moviesDF.select(col("Title"),
    struct(col("US_Gross"),col("Worldwide_Gross"))
    .as("Profit"))
    .select(col("Title"),col("Profit").getField("US_Gross").as("US_Profit")).show()
    //with expression string
  moviesDF.selectExpr("Title","(US_Gross,Worldwide_Gross) as Profit")
    .selectExpr("Title","Profit.US_Gross")

  //Array
val moviesWithWords=moviesDF.select(col("Title"),split(col("Title")," |,").as("Title_Words"))//array of strings
moviesWithWords.select(
  col("Title"),
  expr("Title_Words[0]"),
  size(col("Title_Words")),
  array_contains(col("Title_Words"),"Love")
  )


}





