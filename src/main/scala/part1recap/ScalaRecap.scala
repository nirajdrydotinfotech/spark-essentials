package part1recap

import scala.concurrent.Future
import scala.util.{Failure, Success}

object ScalaRecap extends App{
  //value and variable
  val aBoolean:Boolean=false

  //expression
  val aIfExpression=if(2>3)"bigger" else "smaller"

  //instruction vs expression
  val theUnit=println("hello world") //unit=no meaningful value ==void

  //function
  def myFunction(x:Int)=42

  //oop
  class Animal
  class Cat extends Animal
  trait Carnivore{
    def eat(animal: Animal):Unit
  }

  class Crocodile extends Animal with Carnivore{
    override def eat(animal: Animal): Unit = println("crunch")
  }
  //singleton pattern
  object MySingleton{}

  //companions
  object Carnivore

  //generics
  trait myList[+A]

  //method notation
  val x=1 + 2
  val y=1.+(2)

  //functional programming
  val incrementer:Int=>Int =x=>x+1
  val incremented=incrementer(42)

  //map,flatmap,filter
  val proceesedList=List(1,2,3).map(incrementer)

  //pattern matching
  val unknown:Any=45
  val ordinal=unknown match {
    case 1=> "first"
    case 2 => "second"
    case _ => "unknown"
  }

  //try-catch
  try{
    throw new NullPointerException
  }catch {
    case e:NullPointerException=>"some value"
    case _ =>"something else"
  }
  //futures
  import scala.concurrent.ExecutionContext.Implicits.global
  val aFuture=Future{
    //some expensive computation,runs on another thread
  42
  }
  aFuture.onComplete{
    case Success(value)=>println(s"i found some $value")
    case Failure(exception)=>println(s"i have failed $exception")
  }

  //partial function
  val aPartialFUnction:PartialFunction[Int,Int]={
    case 1=>42
    case 3=>43
    case _ =>999
  }

  //Implicits
  //auto-injection by compiler
  def methodWithImplicitArgument(implicit x:Int)=x+43
  implicit val implicitInt = 67
 val implicitCall=methodWithImplicitArgument

  //implicit conversions- implicit defs
  case class Person(name:String){
    def greet=println(s"hi my name is $name")
  }
  implicit def fromStringToPerson(name:String)=Person(name)
  "Bob".greet  //fromStringToPerson("BOB").greet

  //implicit conversion-implicit class

  implicit class Dog(name:String){
    def bark=println("Bark!")
  }
  "Lassie".bark

  /*
   -local scope
   -imported scope
   -companion object of the type involved in method call
   */
  List(1,2,3).sorted


  def removeN(l:List[Int],n:Int):List[Int]={
    if(n<=0) l
    else
      l.drop(n)
  }
  println(removeN(List(1,2,3,4),2))
}
