package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, exp, expr}


object ColumnAndExpression extends App {

  val spark=SparkSession.builder()
    .appName("DF column and expression")
    .config("spark.master","local")
    .getOrCreate()

  val carsDF=spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/cars.json")

  carsDF.show()

  //column

  val firstColumn=carsDF.col("Name")

  //selecting(projection)
  val carsNameDF=carsDF.select(firstColumn)
  carsNameDF.show()

  //various select methods
  import spark.implicits._
  carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    'year, //scala symbol auto converted to column
    $"Horsepower", //fancier interpolated string,return a column object
    expr("Origin") //expression
  )
  //select with plain column names
  carsDF.select("Name","Year")

  //Expressions
  val simplestExpression=carsDF.col("Weight_in_lbs")
  val weightInKgExpression= carsDF.col("Weight_in_lbs") / 2.2

  val carsWithWeight=carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
  )

  //selectExpr
  val carWithSelectExprWeightsDF=carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "weight_in_lbs / 2.2"
  )
  //DF processing
  //adding a column
  val carsWithKg3DF=carsDF.withColumn("Weight_in_kg_3",col("Weight_in_lbs")/2.2)

  //renaming a column
  val carsWithColumnRenamed=carsDF.withColumnRenamed("Weight_in_lbs","Weight in pounds")
  //careful with column names
  carsWithColumnRenamed.selectExpr("`Weight in pounds`")

  //remove a column
  carsWithColumnRenamed.drop("Cylinders","Displacement")

  //filtering
 val europeanCarsDF=carsDF.filter(col("Origin") =!= "USA")
val europeanCarsDF2=carsDF.where(col("Origin") === "USA")
  //filtering with expression strings
  val americanCarsDF=carsDF.filter("Origin = 'USA'")
//chain filters
  val americanPowerfulCarsDF=carsDF.filter(col("Origin") =!= "USA").filter(col("Horsepower") > 150)
  val americanPowerfulCarsDF2=carsDF.filter(col("Origin") =!= "USA" and col("Horsepower") > 150)
  val americanCarsDF3=carsDF.filter("Origin = 'USA' and Horsepower > 150")

  //unioning ==adding more rows
  val moreCarsDF=spark.read.option("inferSchema","true").json("src/main/resources/data/more_cars.json")
  val allCarsDF=carsDF.union(moreCarsDF) //works if the DFs have the same schema

  //distinct values
  val allCountriesDF=carsDF.select("Origin").distinct()
  allCountriesDF.show()

  carsWithWeight.show()

  /**
    * Exercise
    * 1.read moviesDf and select 2 column of your choice
    * 2.create new DF  summing up the total profit of the movies=US gross +worldwide gross + dvd sale
    * 3.select all the comedy movie with imdb rating above 6
    *
    * use as many version as possible
    *
    */
//1
  val moviesDF=spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/movies.json")

  val selectedColumnDF=moviesDF.select(
    col("Title"),
    col("IMDB_Rating"),
    $"US_Gross",
    expr("Worldwide_Gross")
  )
  val movieReleaseDF=moviesDF.selectExpr(
    "Title","Release_Date"
  )
  selectedColumnDF.show()
//2
  val moviesProfitDF=moviesDF.select(
   col("Worldwide_Gross"),
    col("US_Gross"),
    col( "Worldwide_Gross") + col("US_Gross").as("Total_Gross")
  )
  val moviesProfitDF2=moviesDF.selectExpr(
    "Title",
    "US_Gross",
    "Worldwide_Gross",
    "US_Gross + Worldwide_Gross as Total_Gross"
  )
  val moviesProfitDF3=moviesDF.select("Title","US_Gross","Worldwide_Gross")
    .withColumn("Total_Gross",col("Worldwide_Gross") + col("US_Gross")).show()

  //3
  val comedyMoviesDF=moviesDF.select("IMDB_Rating").filter("IMDB_Rating > 6.0").filter("Major_Genre = 'Comedy'").show()
  val comedyMoviesDF2=moviesDF.select("Title","IMDB_Rating")
    .where(col("Major_Genre") === "Comedy" and  col("IMDB_Rating") > 6.0).show()
val comedyMoviesDF3=moviesDF.select("Title","IMDB_Rating")
  .where("Major_Genre = 'Comedy' and IMDB_Rating> 6").show
}
