// input /Users/Alisa/Documents/docs/important/study/DE/DZ5
package com.example

import org.apache.spark.sql.SparkSession

import org.json4s._
import org.json4s.jackson.JsonMethods._

object JsonReader extends App  {
  val spark = SparkSession
    .builder()
    .master(master = "local[*]")
    .getOrCreate()

  val csFolder = args(0)

  implicit val formats = DefaultFormats

  case class WineRecords(country: Option[String] = None, id:  Option[Double] = None, points: Option[Double] = None, price: Option[Double] = None, title: Option[String] = None,
                        variety: Option[String] = None, winery: Option[String] = None)


  val dataInTheFile = spark.read.json(path = s"$csFolder/winemag-data-130k-v2.json")
  val dataInTheFile2 = spark.sparkContext.textFile(s"$csFolder/winemag-data-130k-v2.json")
 // val decodedLines = dataInTheFile2.map(s => println(parse(s).extract[JsonReader]))
  //val decodedLines = dataInTheFile2.map(s => parse(s).extract[WineRecords]).take(20).foreach(line => println(line.toString))
  val decodedLines = dataInTheFile2.map(s => parse(s).extract[WineRecords]).collect().foreach(line => println(line.toString))



  //dataInTheFile.show()
  //println(dataInTheFile2)
  //println(decodedData2)
 // dataInTheFile2.show()
 // decodedData2.show()
}
