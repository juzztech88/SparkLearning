package com.learning.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/*
* Notes:
* 1. Case class is a compact way of defining a class object, with its members defined in the constructor arguments.
*    The fields can be treated as column names when using Spark SQL.
*
* References:
*
* CASES VS CASE CLASSES:
* https://www.scala-exercises.org/scala_tutorial/classes_vs_case_classes#:~:text=A%20class%20can%20extend%20another,to%20correctly%20implement%20their%20equality).

 */
object SparkSQLDataset_KC {

    // Define the schema of our data with case class
    case class Person(id: Int, name: String, age:Int, friends: Int)

    /** Our main function where the action happens */
    def main(args: Array[String]): Unit ={
        // Set the log level to only print errors
        Logger.getLogger("org").setLevel(Level.ERROR)

        // Use Spark Session interface
        val spark = SparkSession
          .builder
          .appName("SparkSQL_KC")
          .master("local[*]")
          .getOrCreate()

        // Load each line of the source data into an Dataset.
        import spark.implicits._
        val schemaPeople = spark.read
          .option("header", "true") // tell spark that the data has a header row
          .option("inferSchema", "true")    // tell spark to infer the schema for the header row
          .csv("data/fakefriends.csv")  // Here, we create a DATAFRAME
          .as[Person]
        // The .as line CONVERTS THE DATAFRAME TO DATASET, so that we can make use of the
        // type checking at compile time that is only available for dataset.

        // Print out the schema that is a dataset
        schemaPeople.printSchema()

        // Create a database view from the schema dataset.
        // Now, we have a database that is called people in the spark session that we can query on
        schemaPeople.createOrReplaceTempView("people")

        // Query the database named "people"
        val teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

        // Collect the result
        val results = teenagers.collect()

        // Iterate and print out each result
        results.foreach(println)

        // Close the Spark Session when you are done, otherwise your session will keep running
        // in your cluster.
        spark.stop()
    }
}
