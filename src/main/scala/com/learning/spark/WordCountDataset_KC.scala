package com.learning.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{explode, split, lower}

/**
 * 1. explode() - similar to flatmap; explodes columns into rows
 * 2. split(), lower() to preprocess the text
 * 3. When passing column names as parameters, use $ to indicate column name:
 *    split($"value", "\\W+")
 * 4. When ascertaining equality/inequality of column values with something else, use === or =!=
 *    filter($"word" =!= "")
 * 5. For this application, RDD would be a better option because the data is not unstructured.
 *    Dataset is more suited for structured data.
 * 6. Our initial DataFrame will just have Row objects with a column named "value" for each line of
 *    text.
 * 7. You make convert between RDD and dataset later on for easier processing in certain applications.
 */

/** Count up how many of each word occurs in a book, using regular expressions and sorting the final results */
object WordCountDataset_KC {

  // simple case class for our dataset schema
  case class Book(value: String)

  /** Our main function where the action happens */
  def main(args: Array[String]): Unit ={

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a Spark Session using every core of the local machine
    val spark = SparkSession
      .builder
      .appName("WordCountDataset_KC")
      .master("local[*]")
      .getOrCreate()

    // ******************************************************
    // FIRST WAY: USING ONLY DATASET
    // ******************************************************
//    // Read each line of my book into a Dataset
//    import spark.implicits._
//    val input = spark.read.text("data/book.txt").as[Book]
//
//    // Split using a regular expression that extracts words
//    val words = input
//      .select(explode(split($"value", "\\W+")).alias("word"))
//      .filter($"word" =!= "")
//
//    // Normalize everything to lower case
//    val lowercaseWords = words.select(lower($"word").alias("word"))
//
//    // Count up the occurrences of each word
//    val wordCounts = lowercaseWords.groupBy("word").count()
//
//    // Sort by counts
//    val wordCountsSorted = wordCounts.sort("count")
//
//    // Show the results
//    // By default .show only shows the first 20 rows
//    // TO show all the rows, you need to pass in the total number of rows
//    // i.e wordCountsSorted.count.toInt0
//    wordCountsSorted.show(wordCountsSorted.count.toInt)

    // ******************************************************
    // ANOTHER WAY TO DO IT BY BLENDING RDD's AND DATASET
    // ******************************************************
    val bookRDD = spark.sparkContext.textFile("data/book.txt")
    val wordsRDD = bookRDD.flatMap(x => x.split("\\W+"))

    import spark.implicits._  // import this otherwise toDS wouldn't work
    val wordsDS = wordsRDD.toDS() // convert wordsRDD to Dataset

    val lowercaseWordsDS = wordsDS.select(lower($"value").alias("word"))
    val wordCountsDS = lowercaseWordsDS.groupBy("word").count()
    val wordCountsSortedDS = wordCountsDS.sort("count")
    wordCountsSortedDS.show(wordCountsSortedDS.count.toInt)

  }

}
