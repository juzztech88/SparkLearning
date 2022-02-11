package com.learning.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.functions.{size, split, sum}

object MostPopularDS_KC {

  case class SuperHeroNames(id: Int, name: String)
  case class SuperHero(value: String)

  def main(args: Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.ERROR)

    //  Create a Spark session
    val spark = SparkSession
      .builder
      .appName("MostPopularDS_KC")
      .master("local[*]")
      .getOrCreate()

    //  Create schema when reading Marvel-names.txt
    val superHeroNameSchema = new StructType()
      .add("id", IntegerType, nullable = true)
      .add("name", StringType, nullable = true)

    //  Build up a hero ID -> name Dataset
    import spark.implicits._
    val names = spark.read
      .schema(superHeroNameSchema)
      .option("sep", " ")
      .csv("data/Marvel-names.txt")
      .as[SuperHeroNames]

    val lines = spark.read
      .text("data/Marvel-graph.txt")
      .as[SuperHero]

    val connections = lines
      .withColumn("id", split($"value", " ")(0))
      .withColumn("connections", size(split($"value", " ")) - 1)
      .groupBy("id").agg(sum("connections").alias("connections"))

    val mostPopular = connections
      .sort($"connections".desc)
      .first()

    val mostPopularName = names
      .filter($"id" === mostPopular(0))
      .select("name")
      .first()

    println(s"${mostPopularName(0)} is the most popular superhero with ${mostPopular(1)} co-appearances.")

    spark.close()

  }
}
