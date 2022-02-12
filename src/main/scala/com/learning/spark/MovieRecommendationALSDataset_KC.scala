package com.learning.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}
import org.apache.spark.sql.Row
import org.apache.spark.ml.recommendation.{ALS}
import scala.collection.mutable

object MovieRecommendationALSDataset_KC {

  case class MovieNames(movieId: Int, movieTitle: String)
  case class Rating(userId: Int, movieId: Int, rating: Int)

  //  Get movie name by given dataset and id
  def getMovieName(movieNames: Array[MovieNames], movieId: Int): String = {
    //  Reminder: _ is a shortform for x => x, so _.movieId is equivalent to x => x.movieId
    val result = movieNames.filter(_.movieId == movieId)(0)
    result.movieTitle
  }

  /** Our main function where the action happens */
  def main(args: Array[String]): Unit ={
    //  Set the logger level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    //  Make a Spark Session
    val spark = SparkSession
      .builder
      .appName("MoveRecommendationsDS_KC")
      .master("local[*]")
      .getOrCreate()

    println("Loading movie names......")

    //  Create schema when reading u.item
    val moviesNamesSchema = new StructType()
      .add("movieID", IntegerType, nullable=true)
      .add("movieTitle", StringType, nullable=true)

    //  Create schema when reading u.data
    val moviesSchema = new StructType()
      .add("userID", IntegerType, nullable=true)
      .add("movieID", IntegerType, nullable=true)
      .add("rating", IntegerType, nullable=true)
      .add("timestamp", LongType, nullable=true)

    import spark.implicits._
    //  Create a broadcast dataset of movieID and movieTitle
    //  Apply ISO-885901 charset
    val names = spark.read
      .option("sep", "|")
      .option("charset", "ISO-8859-1")
      .schema(moviesNamesSchema)
      .csv("data/ml-100k/u.item")
      .as[MovieNames]

    //  Safely save the movie names in our local machine
    //  as there aren't many movie names in the world
    val namesList = names.collect()

    //  Load up movie data as a dataset
    val ratings = spark.read
      .option("sep", "\t")
      .schema(moviesSchema)
      .csv("data/ml-100k/u.data")
      .as[Rating]

    ratings.show(5)

    //  Build the recommendation model using Alternating Least Squares
    println("\nTraining recommendation model......")

    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)  //  Regularization parameters
      .setUserCol("userID")
      .setItemCol("movieID")
      .setRatingCol("rating")

    val model = als.fit(ratings)

    //  Get top-10 recommendations for the user we specified
    val userID: Int = args(0).toInt
    val users = Seq(userID).toDF("userID")
    val recommendations = model.recommendForUserSubset(users, 10)

    //  Display them (oddly, this is the hardest part!)
    println("\nTop 10 recommendations for user ID " + userID + ":")

    for (userRecs <- recommendations){
      val myRecs = userRecs(1)  //  First column is userID, second is the recommendation
      val temp = myRecs.asInstanceOf[mutable.WrappedArray[Row]] //  Tell Scala what it is
      for (rec <- temp){
        val movie = rec.getAs[Int](0)
        val rating = rec.getAs[Float](1)
        val movieName = getMovieName(namesList, movie)
        println(movieName, rating)
      }
    }

    // Stop the Spark session
    spark.stop()
  }
}
