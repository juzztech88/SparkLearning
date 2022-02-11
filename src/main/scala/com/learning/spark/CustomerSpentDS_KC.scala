package com.learning.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType}
import org.apache.spark.sql.functions.{round,sum}

object CustomerSpentDS_KC {

  case class Customer(user_id: Int, item_id: Int, amt: Double)

  /** Our main class class where all the action happens */
  def main(args: Array[String]): Unit ={

    //  Set logger level to only display errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    //  Create a Spark Session
    val spark = SparkSession
      .builder
      .appName("CustomerSpentDS_KC")
      .master("local[*]")
      .getOrCreate()

    //  Create a schema for the dataset
    val customerSchema = new StructType()
      .add("user_id", IntegerType, nullable=true)
      .add("item_id", IntegerType, nullable=true)
      .add("amt", DoubleType, nullable=true)

    //  Read the dataset
    import spark.implicits._
    val customerDS = spark.read
      .schema(customerSchema)
      .csv("data/customer-orders.csv")
      .as[Customer]

    /** My Solution */
//    // Group by user_id and add up the amount spent
//    val customerAmtDS = customerDS
//      .groupBy("user_id")
//      .sum("amt")
//
//    // Re-format the total amount
//    val formattedCustomerAmtDS = customerAmtDS
//      .withColumn("amt", round($"sum(amt)", 2))
//      .select("user_id", "amt").sort("amt")
//
//    //  Collect results
//    val results = formattedCustomerAmtDS.collect()
//
//    //  Format and print results
//    for (result <- results){
//      val user = result(0)
//      val totalAmt = result(1).asInstanceOf[Double]
//      println(f"Customer $user spent $totalAmt%.2f")
//    }

    /** Kane's solution */
    val customerTotalDS = customerDS
      .groupBy("user_id")
      .agg(round(sum("amt"), 2).alias("total_spent"))

    val customerTotalSortedDS = customerTotalDS
      .sort("total_spent")

    customerTotalSortedDS.show(customerTotalSortedDS.count.toInt)

    spark.stop()
  }
}
