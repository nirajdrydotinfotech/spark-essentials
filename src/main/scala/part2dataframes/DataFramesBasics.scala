package part2dataframes

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

object   DataFramesBasics extends App {
  //creating spark session
  val spark = SparkSession.builder()
    .appName("DataFramesBasics")
    .config("spark.master", "local")
    .getOrCreate()

  //reading dataframe-DF
  val firstDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars.json")

  firstDF.show()
  firstDF.printSchema()

  //get rows
  firstDF.take(50).foreach(println)

  //first spark type
  val longType = LongType

  //schema
  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  //obtain a schema
  val carsDFSchema = firstDF.schema
  // println(carsDFSchema)

  //read a DF with your schema
  val carsDFWithSchema = spark.read
    .format("json")
    .schema(carsDFSchema)
    .load("src/main/resources/data/cars.json")

  //create rows by hand
  val myRow = Row("chevrolet chevelle malibu", 18.0, 8L, 307.0, 130L, 3504L, 12.0, "1970-01-01", "USA")

  //create DF from tuples
  val cars = Seq(
    ("chevrolet chevelle malibu", 18.0, 8L, 307.0, 130L, 3504L, 12.0, "1970-01-01", "USA"),
    ("buick skylark 320", 15.0, 8L, 350.0, 165L, 3693L, 11.5, "1970-01-01", "USA"),
    ("plymouth satellite", 18.0, 8L, 318.0, 150L, 3436L, 11.0, "1970-01-01", "USA"),
    ("amc rebel sst", 16.0, 8L, 304.0, 150L, 3433L, 12.0, "1970-01-01", "USA"),
    ("ford torino", 17.0, 8L, 302.0, 140L, 3449L, 10.5, "1970-01-01", "USA"),
    ("ford galaxie 500", 15.0, 8L, 429.0, 198L, 4341L, 10.0, "1970-01-01", "USA"),
    ("chevrolet impala", 14.0, 8L, 454.0, 220L, 4354L, 9.0, "1970-01-01", "USA"),
    ("plymouth fury iii", 14.0, 8L, 440.0, 215L, 4312L, 8.5, "1970-01-01", "USA"),
    ("pontiac catalina", 14.0, 8L, 455.0, 225L, 4425L, 10.0, "1970-01-01", "USA"),
    ("amc ambassador dpl", 15.0, 8L, 390.0, 190L, 3850L, 8.5, "1970-01-01", "USA")
    )
  val manualCarsDF = spark.createDataFrame(cars) // schema auto-inferred

  //note :DFs have schema,rows do not
  //create DFs with implicits

  import spark.implicits._

  val manualCarDFWithImplicits = cars
    .toDF("Name", "MPG", "Cylinders", "Displacement", "HP", "Weight", "Acceleration", "YearO fOrigin", "Country")

  manualCarsDF.printSchema()
  manualCarDFWithImplicits.printSchema()

  /**
    * Exercise:
    * 1)Create manual DF describing smartPhones
    * -make
    * -model
    * -screen dimension
    * -camera megapixals
    *
    * 2)Read another file from the data foler eg. movies.josn
    * -prints its schema
    * -count the number of rows,call count()
    */

  val smartPhones=Seq(
    ("apple","iphone12",151000,12.5F),
    ("samsung","galaxy",11000,19.5F),
    ("nokia","rocks",10000,22.0F),
    ("asus","rog",51000,41.0F),
    ("oppo","beauty",11000,25.5F),
    ("vivo","vivo-V",11440,20.02F),
    ("oneplus","node",41000,111.8F)
  )
  val manualSmartPhones=spark.createDataFrame(smartPhones)
  val manualPhoneDF=smartPhones.toDF("Make","Model","prize","camera")
manualSmartPhones.show()
  manualPhoneDF.show()
  manualSmartPhones.printSchema()
  manualPhoneDF.printSchema()

  val moviesDF=spark.read
    .format("json")
    .option("inferSchema","true")
    .load("src/main/resources/data/movies.json")

  moviesDF.show()
  moviesDF.printSchema()
 println( moviesDF.count())
}



