package com.learning.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}
import org.apache.spark.sql.functions.{desc, udf, col}

import scala.io.{Codec, Source}


/**
 * Notes:
 * 1. How to link movie ID to its name when displaying result?
 *    An approach would be load the dataset that maps id to name into a dataset.
 *    Then, join this table with the count table.
 *    But doing join operation in distributed manner can be computationally unfriendly.
 *    Plus, there are many movies in the dataset, so we don't need to distribute it.
 *
 * 2. We could:
 *    a.  Keep the movie name dataset in a machine.
 *    b.  Let Spark automatically forward it to each executor when needed.
 *
 * 3. By using broadcast, the table, especially when it is massive, would be
 *    transferred once to each executor and kept there.
 *
 * 4. An object broadcasted to executors will always be there whenever needed.
 *
 * 5. Just use sc.broadcast() to ship off whatever you want. Then, use .value()
 *    to get the object back.
 *
 * 6. You can also use the broadcasted object however you want - map functions,
 *    user defined functions (UDF's) and etc
 */

object PopularMovieDS_KC {

  // Case class so we can get a column name for our movie ID
  final case class Movie(movieID: Int)

  /** Load up a Map of movie IDs to movie names */
  def loadMovieNames(): Map[Int, String]= {
    //  Handle character encoding issues:
    implicit val codec: Codec = Codec("ISO-8859-1") //  This is the current encoding of u.item, not UTF-8

    //  Create a Map of Int to String, and populate it from u.item
    var movieNames: Map[Int, String] = Map()

    val lines = Source.fromFile("data/ml-100k/u.item")
    for (line <- lines.getLines()){
      val fields = line.split("\\|")
      if (fields.length > 1){
        movieNames += (fields(0).toInt -> fields(1))
      }
    }
    lines.close()

    movieNames
  }

  /** Our main function where the action happens */
  def main(args: Array[String]): Unit ={
    // Set the logger level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a Spark Session
    val spark = SparkSession
      .builder
      .appName("PopularMovieDS_KC")
      .master("local[*]")
      .getOrCreate()

    //  Load the mapping between movie ID and name as broadcast object
    val nameDict = spark.sparkContext.broadcast(loadMovieNames())

    //  Create a Schema of out dataset
    val movieSchema = new StructType()
      .add("userID", IntegerType, nullable=true)
      .add("movieID", IntegerType, nullable=true)
      .add("rating", IntegerType, nullable=true)
      .add("timestamp", LongType, nullable=true)

    import spark.implicits._

    //  Load up movie data as dataset
    val movieDS = spark.read
      .option("sep", "\t")
      .schema(movieSchema)
      .csv("data/ml-100k/u.data")
      .as[Movie]  //  Dataset doesn't have to include all the columns in the Dataframe

      /** Count, sort results and then display without mapping movie ID to name */
//    //  Some SQL-style magic to sort all movies by popularity in one line!
//    val topMovieIDs = movieDS.groupBy("movieID").count().orderBy(desc("count"))
//
//    // Grab the top 10
//    topMovieIDs.show(10)

    /**
     * Count results and then map movie ID to name
     */
    val movieCounts = movieDS.groupBy("movieID").count()

    //  Create a user-defined function to look up movie names from our shared Map variable
    //  We start by declaring an "anonymous function" in Scala
    val lookupName: Int => String = (movieID: Int) => {
      nameDict.value(movieID)
    }

    //  Then, wrap it with a udf
    val lookupNameUDF = udf(lookupName)

    //  Add a movieTitle column using our new udf
    val moviesWithNames = movieCounts.withColumn("movieTitle", lookupNameUDF($"movieID"))

    //  Sort the results
    val sortedMoviesWithNames = moviesWithNames.orderBy(desc("count"))

    //  Show the results without truncating the length of each row
    sortedMoviesWithNames.show(sortedMoviesWithNames.count.toInt, truncate=false)

    //  Stop the session
    spark.stop()
  }
}
