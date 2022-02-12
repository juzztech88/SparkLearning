package com.learning.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, explode, slice, split, when}

object DegreeOfSeparationDataset_KC {

  //  The characters we want to find the separation between
  val startCharacterID = 5306 //  SpiderMan
  val targetCharacterID = 14  //  Adam 3,031

  case class SuperHero(value: String)
  case class BFSNode(id: Int, connections: Array[Int], distance: Int, color: String)

  /** Create ïteration 0" of our RDD BFSNodes */
  def createStartingDs(spark: SparkSession): Dataset[BFSNode] ={
    import spark.implicits._

    val inputFile = spark.read
      .text("data/Marvel-graph.txt")
      .as[SuperHero]

    //  Parse the data such as first element will be in column id
    //  and all the rest will be in second column as Array
    val connections = inputFile
      .withColumn("id", split(col("value"), " ")(0).cast("int"))
      .withColumn("connections", slice(split(col("value"), " "), 2, 9999).cast("array<int>"))
      .select("id", "connections")

    //  Add distance and color columns
    val result = connections
      .withColumn("distance",
        when(col("id") === startCharacterID, 0)
          .when(col("id") =!= startCharacterID, 9999)
      )
      .withColumn("color",
        when(col("id") === startCharacterID, "GRAY")
          .when(col("id") =!= startCharacterID, "WHITE")
      ).as[BFSNode]

    result
  }

  /*
      rawExploreDS
      - distinct children of all gray nodes
      -------------
      id	|	child	|

      exploredDs
      - distinct children of all gray nodes
      -----------
      child	|

      exploring
      - after converting all explored gray nodes to black
      -----------
      id	|	connections	|	distance	|	color	|

      result
      - left outer join of exploring with exploreDs
      - with exploring["color"] = "WHITE" and exploring["id"] = exploredDs["child"]
      ----------
      id	|	connections	|	distance	|	color	|	child	|
   */
  def exploreNode(spark: SparkSession, ds: Dataset[BFSNode], iteration: Int): (Dataset[BFSNode], Long) = {
    import spark.implicits._

    val rawExploreDS = ds
      .filter($"color" === "GRAY")
      .select($"id", explode($"connections").alias("child")).distinct()

    val hitCount = rawExploreDS.filter($"child" === targetCharacterID).count()
    val exploreDS = rawExploreDS.distinct().select("child")

    //  All parents become explored after getting exploreDS so we marked these as "BLACK"
    val exploring = ds
      .withColumn("color",
        when(col("color") === "GRAY", "BLACK")
          .otherwise($"color")).as[BFSNode]


    //  Mark all explored nodes on this iteration which were not previously explored
    //  and set distance
    // Basic join operations in SQL: https://www.metabase.com/learn/sql-questions/sql-join-types
    val result = exploring
      .join(exploreDS, exploring("color") === "WHITE" && exploring("id") === exploreDS("child"), "leftouter")
      .withColumn("distance",
        when(col("child").isNotNull, iteration)
          .otherwise($"distance"))
      .withColumn("color",
        when(col("color") === "WHITE" && col("child").isNotNull,  "GRAY")
          .otherwise($"color"))
      .select("id", "connections", "distance", "color").as[BFSNode]

    (result, hitCount)
  }

  /** Our main function where the action happens */
  def main(args: Array[String]): Unit ={
    //  Set the logger level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    //  Create a SparkSession using every core of the local machine
    val spark = SparkSession
      .builder
      .appName("DegreeOfSeparationDS_KC")
      .master("local[*]")
      .getOrCreate()

    //  Character in our BFS traversal
    var hitCount: Long = 0

    // Build dataset
    var iterableDs = createStartingDs(spark)

    for (iteration <- 1 to 10){
      println("Running BFS iteration #" + iteration)
      val resultExplore = exploreNode(spark, iterableDs, iteration)
      iterableDs = resultExplore._1
      hitCount += resultExplore._2

      if (hitCount > 0){
        println("Hit the target character! From " + hitCount + " different direction(s).")
        return
      }
    }
  }
}
