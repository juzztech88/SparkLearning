package com.learning.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{size, split, sum, udf}
import org.apache.spark.sql.types.IntegerType

import scala.io.{Codec, Source}

object LeastPopularDS_KC {

  case class SuperHero(value: String)

  def loadHeroNames() : Map[Int, String] = {
    //  Handle character encoding issues
    implicit val codec: Codec = Codec("ISO-8859-1")

    //  Create a Map of Int to String, and populate it from Marvel-names.txt
    var heroNames: Map[Int, String] = Map()

    val lines = Source.fromFile("data/Marvel-names.txt")
    for (line <- lines.getLines()){
      val fields = line.split("\"")
      if (fields.length > 1){
        heroNames += (fields(0).trim().toInt -> fields(1))
      }
    }
    lines.close()
    heroNames
  }

  def main(args: Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("LeastPopularDS_KC")
      .master("local[*]")
      .getOrCreate()

    val nameDict = spark.sparkContext.broadcast(loadHeroNames())

    import spark.implicits._
    val lines = spark.read
      .text("data/Marvel-graph.txt")
      .as[SuperHero]

    val connections = lines
      .withColumn("hero_id", split($"value", " ")(0).cast(IntegerType))
      .withColumn("connections", size(split($"value", " ")) - 1)
      .groupBy("hero_id").agg(sum("connections").alias("connections"))
      .select("hero_id", "connections")
      .sort($"connections")

    //  Get the least number of connections
    val leastTotalConnection = connections.first()(1)

    //  Anonymous user-defined function to map hero id to name
    val lookupName: Int => String = (heroID: Int) => {
      nameDict.value(heroID)
    }

    // Wrap the anonymous function with a udf
    val lookupNameUDF = udf(lookupName)

    // Add heroName column using the udf
    val connectionsWithNames = connections
      .withColumn("heroName", lookupNameUDF($"hero_id"))

    // Filter out heros having the least number of connections
    val leastPopularHeros = connectionsWithNames
      .filter($"connections" === leastTotalConnection)
      .sort("hero_id")
      .select("heroName")

    println(s"Heros having the least number of connections - ${leastTotalConnection}:")
    leastPopularHeros.show(leastPopularHeros.count.toInt, truncate=false)

    spark.close()
  }
}
