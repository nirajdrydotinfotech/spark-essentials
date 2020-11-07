package part3typesdataset

import java.sql.Date

import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._

object Datasets extends App {

  val spark=SparkSession.builder()
    .appName("Datasets")
    .config("spark.master","local")
    .getOrCreate()


  val numberDF:DataFrame=spark.read.option("inferSchema","true").option("header","true").csv("src/main/resources/data/numbers.csv")
  numberDF.printSchema()
  numberDF.filter(col("numbers") >100)

  //convert df to ds
  implicit val intEncoder=Encoders.scalaInt
  val numberDS:Dataset[Int]=numberDF.as[Int]

  //datasets for complex type
  //1.define your case class
  case class Car(
                  Name:String,
                  Miles_per_Gallon:Option[Double],
                  Cylinders:Long,
                  Displacement:Double,
                  Horsepower:Option[Long] ,
                  Weight_in_lbs:Long,
                  Acceleration:Double,
                  Year:String,
                  Origin:String
                )
  //2.read df from the file
  def readDB(fileName:String)=spark.read
    .option("inferSchema","true")
    .json(s"src/main/resources/data/$fileName")

  //3.define an encoder (importing the implicits)
  import spark.implicits._

  val carsDF=readDB("cars.json")
//  implicit val carEncoder=Encoders.product[Car]
  //4.convert the df to ds
  val carsDS=carsDF.as[Car]

  //ds collection functions
  numberDS.filter(_ < 100).show()

  //map,flatmap,fold,reduce,for comprehension ...
  val canNameDS=carsDS.map(car=>car.Name.toUpperCase())
  canNameDS.show()


  /**
    * Exercise
    * 1.count how many cars we have
    * 2.count how many powerful cars we have(HP>140)
    * 3.average HP for the entire dataset
    */

  //1
  println(carsCount)
  val carsCount=carsDS.count

  //2
  println(carsDS.filter(_.Horsepower.getOrElse(0L) > 140).count)

  //3
  println(carsDS.map(_.Horsepower.getOrElse(0L)).reduce(_+_)/carsCount)

  //also use the DF functions
carsDS.select(avg(col("Horsepower"))).show()

  //joins
  case class Guitar(id:Long,make:String,model:String,guitarType:String)
  case class GuitarPlayer(id:Long,name:String,guitars:Seq[Long],band: Long)
  case class Band(id:Long,name:String,hometown:String,year:Long)
  val guitarsDS=readDB("guitars.json").as[Guitar]
  val guitarPlayerDS=readDB("guitarPlayers.json").as[GuitarPlayer]
  val bandsDS=readDB("bands.json").as[Band]

  val guitarPlayerBandsDs:Dataset[(GuitarPlayer,Band)]=guitarPlayerDS
    .joinWith(bandsDS,guitarPlayerDS.col("band") === bandsDS.col("id"),"inner")
guitarPlayerBandsDs.withColumnRenamed("_1","GuitarsPlayers").withColumnRenamed("_2","Bands").show()

  /**
    * Exercise-join the guitarsDS and guitarPlayersDS,in an outer join
    * (hint:use array_contains)
    */

  val guitarPlayerWithGuitarsDS=guitarPlayerDS.joinWith(guitarsDS,
    array_contains(guitarPlayerDS.col("guitars"),
      guitarsDS.col("id")),"outer").show()

  //grouping DS
  val carsGroupedByOrigin=carsDS
    .groupByKey(_.Origin)
    .count()
    .show()

  //join and groups are WIDE transformation ,will involve SHUFFLE operations
}
