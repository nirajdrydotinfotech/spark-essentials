package part5lowlevel

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.io.Source

object RDDs extends App {

  val spark=SparkSession.builder()
    .appName("Introduction to RDDs")
    .config("spark.master","local")
    .getOrCreate()

  val sc=spark.sparkContext

  //1.parallelize an existing connection
  val numbers=1 to 100000
  val numbersRDD=sc.parallelize(numbers)

  //2.reading from the files
  case class StockValues(symbol:String,date:String,price:Double)
  def readStocks(fileName:String)=
    Source.fromFile(fileName)
      .getLines()
      .drop(1)
      .map(line=>line.split(","))
      .map(tokens=>StockValues(tokens(0),tokens(1),tokens(2).toDouble))
      .toList

  val stockRDD=sc.parallelize(readStocks("src/main/resources/data/stocks.csv"))

  //2b-reading file
  val stocksRDD2=sc.textFile("src/main/resources/data/stocks.csv")
    .map(line=>line.split(","))
    .filter(tokens=>tokens(0).toUpperCase() == tokens(0))
    .map(tokens=>StockValues(tokens(0),tokens(1),tokens(2).toDouble))

  //3.read from dataframe
  val stocksDF=spark.read
    .option("header","true")
    .option("inferSchema","true")
    .csv("src/main/resources/data/stocks.csv")

  //val stocksRDD4=stocksDF.rdd  //you obtain rdd of raw bcs stocksdf is df not ds
  import spark.implicits._
  val stocksDS=stocksDF.as[StockValues]
  val stocksRDD3=stocksDS.rdd

  //RDD ->DF
  val numberDF=numbersRDD.toDF("number") //you loose the type info

  //rdd->ds
  val numberDS=spark.createDataset(numbersRDD) //you get to keep type info

  //Transformation
  val msftRDD=stockRDD.filter(_.symbol == "MSFT") //lazy transformation

  //counting
  val msCount= msftRDD.count() //eager action
  val companyNamesRDD=stockRDD.map(_.symbol).distinct()  //lazy transformation

  //min and max
  implicit val stocksOrdering:Ordering[StockValues]=
    Ordering.fromLessThan[StockValues]((sa:StockValues,sb:StockValues)=>sa.price < sb.price) //different three value to help complier to infer type of stocks
  val minMsft=msftRDD.min() //action

  //reduce
  numbersRDD.reduce(_ + _)

  //grouping
  val groupedStocksRDD=stockRDD.groupBy(_.symbol)
  //grouping is expensive

  //partitioning
  val repartitionedStocksRDD=stockRDD.repartition(30)
  repartitionedStocksRDD.toDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks30")

  /*
  repartitioning is expensive.involves shuffling.
  best practice:partition early ,then process that.
  size of partition is between 10-100MB
     */

  //coalesce
  val coalesceRDD=repartitionedStocksRDD.coalesce(15) //does not involve shuffling

  coalesceRDD.toDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks15")

  /**
    * Exercises
    *
    *1.read movie json as an RDD.
    *2.show the distinct genre as an RDD
    *3.select all the movie in the drama genre with imdb rating>6
    *4.show the average rating of movies by genre.
    */

  case class Movie(title:String,genre:String,rating:Double)

  val movieDF=spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/movies.json")

  val movieRDD=movieDF
    .select(col("Title").as("title"),
      col("Major_Genre").as("genre"),
      col("IMDB_Rating").as("rating"))
    .where(col("genre").isNotNull and col("rating").isNotNull)
    .as[Movie]
    .rdd

  //2.
  val genreRDD=movieRDD.map(_.genre).distinct()

  //3.
  val goodDramaMovieRDD=movieRDD.filter(movie=>movie.rating >6.0 && movie.genre=="Drama")

  movieRDD.toDF().show()
  genreRDD.toDF().show()
  goodDramaMovieRDD.toDF().show()

  //4.
  case class GenreAVGRating(genre:String,rating:Double)
  val avgMovieRDD=movieRDD.groupBy(_.genre).map{
    case (genre,movies)=>GenreAVGRating(genre, movies.map(_.rating).sum /movies.size)
  }
  avgMovieRDD.toDF().show()
}


