package com.ryde

import org.apache.spark.sql.{SparkSession, functions}

import org.apache.log4j.Logger
import org.apache.log4j.Level

object Consumer {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val spark= SparkSession.builder
      .appName("StructuredStream")
      .master("local[2]")
      .getOrCreate()
import spark.implicits._

    val smallBatch = spark.read.format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "sample")
      .option("startingOffsets", "earliest")
      .option("endingOffsets", """{"sample":{"0":2}}""")
      .load()
      .selectExpr("CAST(value AS STRING) as STRING").as[String].toDF()

    smallBatch.write.mode("overwrite").format("text").save("D:\\work\\test\\sample")

    val smallBatchSchema = spark.read.json("/batch/batchName.txt").schema

 /*   val inputDf = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "node:9092")
      .option("subscribe", "topicName")
      .option("startingOffsets", "earliest")
      .load()

    val dataDf = inputDf.selectExpr("CAST(value AS STRING) as json")
      .select( from_json($"json", schema=smallBatchSchema).as("data"))
      .select("data.*")*/

    /*val inputDf = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("zookeeper.connect", "localhost:2181")
      .option("subscribe", "sample")
      .option("startingOffsets", "earliest")
      .option("max.poll.records", 10)
      .option("failOnDataLoss", false)
      .load()
inputDf.printSchema()
    val personJsonDf = inputDf.selectExpr("CAST(value AS STRING)")
    personJsonDf.printSchema()

    personJsonDf.writeStream
      .format("console")
      .start()*/
  }
}
