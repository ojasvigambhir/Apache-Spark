package com.spark.scala.dwh

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}




object cdc extends App{
  
Logger.getLogger("org").setLevel(Level.OFF)
Logger.getLogger("akka").setLevel(Level.OFF)
  System.setProperty("hadoop.home.dir", "D:\\winutils");
  val spark = SparkSession.builder.master("local").appName("Change Data Capture").getOrCreate()
 // spark.setLogLevel("ERROR")
  val sourceDF    = spark.read.format("csv").option("header","true").load("C:\\Users\\ojasvi.gambhir\\workspace\\git\\Apache-Spark\\ApacheSpark\\src\\main\\resources\\movies.csv")
  sourceDF.show(false)
}