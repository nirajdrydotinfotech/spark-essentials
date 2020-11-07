package part3typesdataset

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CommonTypes extends App{

  val spark =SparkSession.builder()
    .appName("Common data types")
    .config("spark.master","local")
    .getOrCreate()

  val moviesDF=spark.read.
    option("inferSchema","true")
    .json("src/main/resources/data/movies.json")

  //adding a plain value to a df
  moviesDF.select(col("Title"),lit(47).as("plain_value"))

  //booleans
  val dramaFilter=col("Major_Genre") equalTo "Drama"
  val goodRating=col("IMDB_Rating") > 7.0
  val preferredFilter=dramaFilter and goodRating

  moviesDF.select("Title").where(dramaFilter)
  //+ multiple way of filtering

  val moviesWithGoodnessFlagsDF=moviesDF
    .select(col("Title"),preferredFilter.as("good_movie"))
  //filter on a boolean column name
  moviesWithGoodnessFlagsDF.where("good_movie").show() //where(col("good_movie") === true)

  //negations
  moviesWithGoodnessFlagsDF.where(not(col("good_movie"))).show()

  //numbers
  //math operators
  val moviesAVGRatingDF=moviesDF.select(col("Title"),
    (col("Rotten_Tomatoes_Rating")/10 + col("IMDB_Rating"))/2)

  //correlation=number between -1 and 1
  println(moviesDF.stat.corr("Rotten_Tomatoes_Rating","IMDB_Rating")) // corr is an action

  //strings
  val carsDF=spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/cars.json")

  //capitalization -init,lower ,upper
  carsDF.select(initcap(col("Name"))).show()

  //contains
  carsDF.select("*").where(col("Name").contains("volkswagen"))

  //regex
  val regexString="volkswagen|vw"
  val vwDF=carsDF.select(
    col("Name"),
    regexp_extract(col("Name"),regexString,0).as("regex_extract")
  ).where(col("regex_extract") =!= "").drop("regex_extract")

  vwDF.select(
    col("Name"),
    regexp_replace(col("Name"),regexString,"People's Car")
      .as("regex_replace")).show()

  /**
    * Exercise
    *
    * filter the car DF by a list of cars names obtained by an API call
    * versions:
    *   -contains
    *   -regex
    */
  def getCarNames:List[String]= List("volkswagen","Chevrolet","Ford")
  val complexRegex=getCarNames.map(_.toLowerCase).mkString("|")

  //version 1-regex
  val complexCarsDF=carsDF.select(
    col("Name"),
    regexp_extract(col("Name"),complexRegex,0).as("regex_extract")
  ).where(col("regex_extract") =!= "")
    .drop("regex_extract")

  //version 2-contains
  val carNameFilters=getCarNames.map(_.toLowerCase).map(name=>col("Name").contains(name))
  val bigFilter=carNameFilters
    .fold(lit(false))((combinedFilter,newCarNameFilter) => combinedFilter or newCarNameFilter)
  carsDF.filter(bigFilter).show()

}
