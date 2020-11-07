package part2dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}

object DataSources extends App {

  val spark=SparkSession.builder()
    .appName("baba here for dataSource")
    .config("spark.master","local")
    .getOrCreate()


  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  /**
    * Reading DF:
    * -format
    * -schema or infersSchema=true
    * -zero or more option
    * -path
    * -laod(path to file)
    */
  val carsDF=spark.read
    .format("json")
    .schema(carsSchema)
    .option("mode","failFast")//dropMalformed,permissive(default)
    .option("path","src/main/resources/data/cars.json")
    .load()

  //alternative with option map
  val carsDFWithOptionMap=spark.read
    .format("json")
    .options(Map(
      "mode"->"failFast",
      "path"->"src/main/resources/data/cars.json",
      "inferSchema"->"true"
    ))
    .load()

  /*writing DFs
      -format
      -save mode=overwrite or append or ignore or errorIfExists
      -path
      -zero or more options
   */

  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    //.option("path","src/main/resources/data/cars_duplicate.json")
    .save("src/main/resources/data/cars_duplicate.json")

 //carsDF.show()  //action

  //json flags
  spark.read
    .schema(carsSchema)
    .option("dateFormat","YYYY-MM-dd") //couple with schema ;if spark fails parsing,it will put pull
    .option("allowSingleQuotes","true")
    .option("compression","uncompressed")  //bzip2,gzip,lz4,snappy,deflate
    .json("src/main/resources/data/cars.json")

  //csv flags
  val stocksSchema=StructType(Array(
    StructField("symbol",StringType),
    StructField("date",DateType),
    StructField("price",DoubleType)
  ))

  spark.read
    .schema(stocksSchema)
    .option("dateFormat","MMM dd YYYY")
    .option("header","true")
    .option("sep",",")
    .option("nullValue","")
    .csv("src/main/resources/data/stocks.csv")

  //parquet

  carsDF.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars.parquet")

  //text file

  spark.read
    .text("src/main/resources/data/sampleTextFile.txt").show()
  val driver="org.postgresql.Driver"
  val url="jdbc:postgresql://localhost:5432/rtjvm"
  val user="docker"
  val password="docker"
//reading form remote database
/* val employeesDF= spark.read
    .format("jdbc")
    .option("driver",driver)
    .option("url",url)
    .option("user",user)
    .option("password",password)
    .option("dbtable","public.employees")
    .load()*/
//employeesDF.show()

  /**
    * Exericse:read the movies dataframe ,then write it as
    * -tab-seperated values file
    * -snappy parquet
    * -table public.movies in postgressDB
    *
   */

 val moviesDF=spark.read
  //   .option("inferSchema","true")
    .json("src/main/resources/data/movies.json")

  moviesDF.write
    .format("csv")
    .option("header","true")
    .option("sep","\t")
    .save("src/main/resources/data/newmovies.csv")

  moviesDF.write.save("src/main/resources/data/newmovies.parquet")

  moviesDF.write
    .format("jdbc")
    .options(Map(
      "driver"->driver,
      "url"->url,
      "user"->user,
      "password"->password,
      "dbtable"->"public.movies"
    )).save()

}
