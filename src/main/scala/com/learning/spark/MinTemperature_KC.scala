package com.learning.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.math.min

object MinTemperature_KC {
  def parseLine(line: String): (String, String, Float) = {
    val fields = line.split(",")
    val stationID = fields(0)
    val entryType = fields(2)
    val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (stationID, entryType, temperature)
  }

  def main(args : Array[String]): Unit ={
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a Spark Context using every core of the local machine
    val sc = new SparkContext("local[*]", "MinTemperatures")

    // Read each line of input data
    val rdd = sc.textFile("C:/Users/ckc3b/Documents/SparkLearningKC/data/1800.csv")

    // Convert to (stationID, entryType, temperature) tuples
    val parsedLines = rdd.map(parseLine)

    // Filter out all but TMIN entries
    val minTemps = parsedLines.filter(x => x._2 == "TMIN")

    // Convert to (stationID, temperature)
    val stationTemps = minTemps.map( x => (x._1, x._3.toFloat))

    // Reduce by stationID retaining the minimum temperature found
    val minTempsByStation = stationTemps.reduceByKey((x, y) => min(x, y))

    // collect the results
    val results = minTempsByStation.collect()

    // sort and print the results
    for (res <- results.sorted) {
      println(f"${res._1} minimum temperature: ${res._2}%.2f F.")
    }

  }
}
