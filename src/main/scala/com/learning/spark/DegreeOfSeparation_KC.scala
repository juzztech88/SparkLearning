package com.learning.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable.ArrayBuffer

/**
 * Finds the degree of separation between two Marvel comic book characters,
 * based on co-appearances in a comic.
 *
 * Referemce:
 * 1. Using Option:
 * https://www.baeldung.com/scala/option-type
 */

object DegreeOfSeparation_KC {

  //  The character we want to find the separation between
  val startCharacterID = 5306 // SpiderMan
  val targetCharacterID = 14  // ADAM 3,031

  //  We make out accumulator a "global" Option so we can reference it in a mapper later
  // https://www.baeldung.com/scala/option-type
  var hitCounter: Option[LongAccumulator] = None

  //  Some custom data types
  //  BFSData contains an array of hero ID connections, the distance, and color
  type BFSData = (Array[Int], Int, String)
  //  A BFSNode has a heroID and the BFSData associated with it
  type BFSNode = (Int, BFSData)

  /** Converts a line of raw input into a BFSNode */
  def converToBFS(line: String): BFSNode = {
    //  Split up the line into fields
    val fields = line.split("\\s+")

    //  Extract this heroID from the frist field
    val heroID = fields(0).toInt

    //  Extract subsequent hero ID's into the connections array
    var connections: ArrayBuffer[Int] = ArrayBuffer()
    for (connection <- 1 until fields.length){
      connections += fields(connection).toInt
    }

    //  Default distance is infinity = 9999 and color is white
    var color: String = "WHITE"
    var distance: Int = 9999

    //  Unless this is the character we're starting from
    if (heroID == startCharacterID){
      color = "GRAY"
      distance = 0
    }

    (heroID, (connections.toArray, distance, color))
  }
  /** Create "iteration0" of our RDD of BFSNodes */
  def createStartingRdd(sc:SparkContext): RDD[BFSNode] = {
    val inputFile = sc.textFile("data/Marvel-graph.txt")
    inputFile.map(converToBFS)
  }

  /** Expand a BFSNode into this node and its children */
  def bfsMap(node: BFSNode): Array[BFSNode] = {
    //  Extract data from the BFSNode
    val characterID: Int = node._1
    val data: BFSData = node._2

    val connections: Array[Int] = data._1
    val distance: Int = data._2
    val color: String = data._3

    //  This is called from flatMap, so we return an array
    //  of potentially many BFSNodes to add to our new RDD
    var results: ArrayBuffer[BFSNode] = ArrayBuffer()

    //  Gray nodes are flagged for expansion, and create new
    //  gray nodes for each connection
    if (color == "GRAY"){
      for (connection
           )
    }
  }


  /** Our main function where the action happens */
  def main(args: Array[String]): Unit ={
    //  Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    //  Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "DegreesOfSeparation_KC")

    //  Our accumulator, used to signal when we find the target
    //  character in our BFS traversal
    //  https://www.baeldung.com/scala/option-type
    hitCounter = Some(sc.longAccumulator("Hit Counter"))

    var iterationRdd = createStartingRdd(sc)

    for (iteration <- 1 to 10){
      println("Running BFS Iteration# " + iteration)

      //  Create new vertices as needed to darken or reduce distances in the reduce stage.
      // If we encounter the node we're looking for as a GRAY node, increment our accumulator
      //  to signal that we are done
      val mapped = iterationRdd.flatMap(bfsMap)

    }
  }
}
