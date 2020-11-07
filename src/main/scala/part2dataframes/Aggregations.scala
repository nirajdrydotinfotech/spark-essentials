package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{approx_count_distinct, avg, col, count, countDistinct, max, mean, min, stddev, sum}

object Aggregations  extends App {

  val spark = SparkSession.builder()
    .appName("Aggregation and grouping")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  //counting
  val genresCountDF = moviesDF.select(count(col("Major_Genre"))) //all the value except null
  //genresCountDF.show()
  moviesDF.selectExpr("count(Major_Genre)")
  //counting all
  moviesDF.select(count("*")).show() //count all the rows,and will include nulls

  //counting distinct value
  moviesDF.select(countDistinct(col("Major_Genre"))).show()

  //approximate count
  moviesDF.select(approx_count_distinct(col("Major_Genre"))).show()

  //min and max
  val minRating = moviesDF.select(min("IMDB_Rating")).show()
  moviesDF.selectExpr("max(IMDB_Rating)").show()

  //sum
  moviesDF.select(sum("US_Gross")).show()
  moviesDF.selectExpr("sum(US_Gross)").show()

  //avg
  moviesDF.select(avg("IMDB_Rating")).show()

  //data science
  moviesDF.select(
    mean("Rotten_Tomatoes_Rating"),
    stddev("Rotten_Tomatoes_Rating"))

  //grouping
  val countByGenresDF = moviesDF
    .groupBy("Major_Genre")
    .count() //select count(*) from moviesDF group by Major_Genre

  val avgRatingByGenresDF = moviesDF
    .groupBy(col("Major_Genre"))
    .avg("IMDB_Rating")

  val aggregationByGenres = moviesDF
    .groupBy(col("Major_Genre"))
    .agg(
      count("*").as("N_Movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    ).orderBy(col("Avg_Rating")).show()


  /**
    * Exercise
    * 1.sum up all the profit of all the movies in the DF
    * 2.count how many distinct directors we have
    * 3.show the mean and standard deviation of us gross revenue for the movies
    *4find out the avg of imdb rating and avg us gross revenue per director
    */

    val sumOfProfitDF=moviesDF.select(sum(col( "Worldwide_Gross") + col("US_Gross")).as("Total_Profit")).show()

  moviesDF.select(countDistinct("Director")).show()
  moviesDF.select(mean("US_Gross"),
    stddev("US_Gross"))
    .show()
  moviesDF.groupBy(col("Director"))
    .agg(
    avg("IMDB_Rating").as("Avg_Rating"),
      sum("US_Gross").as("Total_US_Gross")
  )
    .orderBy(col("Avg_Rating").desc_nulls_last)
    .show()
}