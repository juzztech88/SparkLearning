package com.learning.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object FakeFriendsDataset_KC {

  // Define the schema of our dataset
  case class Person(id: Int, name: String, age: Int, friends: Int)

  /** Our main function where the action happends */
  def main(args: Array[String]): Unit ={
    // Set the logger level to only print erros
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Use Spark Session interface
    val spark = SparkSession
      .builder
      .appName("FakeFriendsDataset_KC")
      .master("local[*]")
      .getOrCreate()

    // Convert our csv file to a Dataset, using our Person class
    // to infer the schema
    import spark.implicits._
    val people = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/fakefriends.csv")
      .as[Person]

    // Print the dataset schema
    println("Here is our inferred schema: ")
    people.printSchema()

    // Group by age
    println("Average number of friends per age: ")
    people.groupBy("age").avg("friends").sort("age").show()

    // Stop the Spark Session
    spark.stop()
  }
}
