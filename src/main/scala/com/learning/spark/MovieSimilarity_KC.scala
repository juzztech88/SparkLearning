package com.learning.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import java.nio.charset.CodingErrorAction
import scala.io.{Codec, Source}
import scala.math.sqrt

object MovieSimilarity_KC {
  /**
   * Item-based Collaborative Filtering
   * E.g. Two movies are often rated similarly by many users. Therefore, they are likely similar to each other.
   * If a person happens to watch one of the movies but unaware of the other, the algorithm would recommend
   * the other movie to this person.
   *
   * This script implements Item-based Collaborative Filtering in Spark.
   *
   * Approach:
   * 1. Map input ratings to (userID, (movieID, rating))
   * 2. Find every movie pair rated by the same user
   *    > This can be done with a "self-join" operation
   *    > At this point, we have (userID, ((movieID1, rating1), (movieID2, rating2)) and the permutations
   *    > of the values i.e. ((movieID1, rating1), (movieID2, rating2), (movieID2, rating2), (movieID1, rating1))
   * 3. Filter out the duplicate pairs which arise from the permutations.
   * 4. Make the movie pairs the key
   *    > map to ((movieID1, movieID2), (rating1, rating2))
   * 5. groupByKey() to get every rating pair found for each movie pair
   * 6. Compute similarity between ratings for each movie pair
   * 7. Sort, save and display results.
   *
   * Technical notes:
   * 1. Every time you perform more than one action on an RDD, you must cache it!
   * 2. Otherwise, Spark might re-evaluate the entire RDD all over again!
   * 3. User .cache() or .persist() to do this
   * 4. The difference between the two is that .persist() optionally lets you cache it to disk instead of
   *    just memory, just in case a node fails or something.
   */

  /** Load up a map of movie IDs to movie names */
  def loadMovieNames() : Map[Int, String] = {

    // Handle character encoding issues
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a Map of Ints to Strings, and populate it from u.item
    var movieNames: Map[Int, String] = Map()

    val lines = Source.fromFile("C:/Users/ckc3b/Documents/SparkLearningKC/data/ml-100k/u.item")
    for (line <- lines.getLines()){
      val fields = line.split('|')
      if (fields.length >  1){
        movieNames += (fields(0).toInt -> fields(1))
      }
    }
    movieNames
  }

  type MovieRating = (Int, Double)  // corresponding to (movieID, movie_rating)
  type UserRatingPair = (Int, (MovieRating, MovieRating)) // corresponding to (userID, (movieRatingPair1, movieRatingPair2))

  def makePairs(userRatings: UserRatingPair) = {
    val movieRating1 = userRatings._2._1
    val movieRating2 = userRatings._2._2

    val movie1 = movieRating1._1
    val rating1 = movieRating1._2
    val movie2 = movieRating2._1
    val rating2 = movieRating2._2

    ((movie1, movie2), (rating1, rating2))
  }

  def filterDuplicates(userRatings:UserRatingPair): Boolean = {
    val movieRating1 = userRatings._2._1
    val movieRating2 = userRatings._2._2

    val movie1 = movieRating1._1
    val movie2 = movieRating2._1

    movie1 < movie2
  }

  type RatingPair = (Double, Double)
  type RatingPairs = Iterable[RatingPair]

  def computeCosineSimilarity(ratingPairs: RatingPairs): (Double, Int) = {
    var numPairs: Int = 0
    var sum_xx: Double = 0.0
    var sum_yy: Double = 0.0
    var sum_xy: Double = 0.0

    for (pair <- ratingPairs){
      val ratingX = pair._1
      val ratingY = pair._2

      sum_xx += ratingX * ratingX
      sum_yy += ratingY * ratingY
      sum_xy += ratingX * ratingY
      numPairs += 1
    }

    val numerator: Double = sum_xy
    val denominator = sqrt(sum_xx) * sqrt(sum_yy)

    var score: Double = 0.0
    if (denominator != 0){
      score = numerator / denominator
    }

    (score, numPairs)
  }

  def main(args: Array[String]) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MovieSimilarityKC")

    println("\nLoading movie names......")
    val nameDict = loadMovieNames()

    val data = sc.textFile("C:/Users/ckc3b/Documents/SparkLearningKC/data/ml-100k/u.data")

    // Map ratings to key / value pairs: userID => movieID, rating
    val ratings = data.map(l => l.split("\t")).map(l => (l(0).toInt, (l(1).toInt, l(2).toDouble)))

    // Emit every movie rated together by the same user.
    // Self-join to find every permutation. (A, B), (B, A), (A, A), (A, A), (B, B), (B, B)
    val joinedRatings = ratings.join(ratings)

    // At this point our RDD consists of userID => ((movieID, rating), (movieID, rating))

    // Filter out duplicate pairs
    val uniqueJoinedRatings = joinedRatings.filter(filterDuplicates)

    // Now key by (movie1, movie2) pairs, userID was omitted
    val moviePairs = uniqueJoinedRatings.map(makePairs)

    // We now have (movie1, movie2) => (rating1, rating2)
    // Now collect all ratings for each movie pair.
    val moviePairRatings = moviePairs.groupByKey()

    // We now have (movie1, movie2) => (rating1, rating2), (rating1, rating2), ...
    // We can now compute similarities
    // We are going to use this RDD more than once, so we cache it so that we don't need to re-run it
    val moviePairSimilarities = moviePairRatings.mapValues(computeCosineSimilarity).cache()

    // Extract similarities for the movie we care about that are "good"
    if (args.length > 0){
      val scoreThreshold = 0.97
      val coOccurrenceThreshold = 50.0

      val movieID: Int = args(0).toInt

      // Filter for movies with this similarity that are "good" as defined by
      // our quality thresholds above

      val filteredResults = moviePairSimilarities.filter( x =>
      {
        val pair = x._1
        val sim = x._2
        (pair._1 == movieID || pair._2 == movieID && sim._1 > scoreThreshold && sim._2 > coOccurrenceThreshold)
      })

      // Sort by quality score
      val results = filteredResults.map(x => (x._2, x._1)).sortByKey(ascending=false).take(10)

      println("\nTop 10 similar movies for " + nameDict(movieID))
      for (result <- results){
        val sim = result._1
        val pair = result._2

        // Display the similarity result that isn't the movie we are looking at
        var similarMovieID = pair._1
        if (similarMovieID == movieID){
          similarMovieID = pair._2
        }
        println(nameDict(similarMovieID) + "\tscore: " + sim._1 + "\tstrength: " + sim._2)
      }
    }
  }
}
