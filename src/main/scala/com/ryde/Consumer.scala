package com.ryde

import java.util
import java.util.ArrayList

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import com.ryde.SaveToCassandra
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, StructField, StringType}

object Consumer {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> "org.apache.kafka.common.serialization.ByteArrayDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> "sample.group",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val sparkConf = new SparkConf().setMaster("local[2]")
      .setAppName("StreamingConsumer")
      .set("spark.cassandra.connection.host", "127.0.0.1")
    //                    .set("spark.io.compression.codec", "snappy");
    //                    .options("spark.driver.allowMultipleContexts", true)

    val spark = SparkSession.builder
      .appName("StructuredStream")
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext
    val streamingContext = new StreamingContext(sc, Seconds(2))
    val saveOb = new SaveToCassandra

    val topics = Array("business")
    val dStream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      LocationStrategies.PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val messages = dStream.map(ConsumerRecord => ConsumerRecord.value())
    import spark.implicits._

    messages.foreachRDD(
      rdd => {
        val df = spark.read.json(rdd.toDS())
        val saveDf = df.select("business_id", "name", "address","city","state","postal_code","stars" )
        saveOb.appendToCassandraTableDF(saveDf,topics(0))
      })

    val topic2 = Array("checkin") //"photo","review","tip","user")
    val checkInStream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      LocationStrategies.PreferConsistent,
      Subscribe[String, String](topic2, kafkaParams)
    )

    val checkInMessages = checkInStream.map(ConsumerRecord => ConsumerRecord.value())

    checkInMessages.foreachRDD (
      checkInRdd => {
        val checkInDf = spark.read.json(checkInRdd.toDS())
//        checkInDf.printSchema()
        val saveCheckInDf = checkInDf.select("business_id","date")
        saveOb.appendToCassandraTableDF(saveCheckInDf,topic2(0))
      })

    val topic3 = Array("photo") //"photo","review","tip","user")
    val photoStream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      LocationStrategies.PreferConsistent,
      Subscribe[String, String](topic3, kafkaParams)
    )

    val photoMessages = photoStream.map(ConsumerRecord => ConsumerRecord.value())
    import spark.implicits._

    photoMessages.foreachRDD (
      photoRdd => {
        val photoDf = spark.read.json(photoRdd.toDS())
//        photoDf.printSchema()
        val savephotoDf = photoDf.select("business_id","caption", "label","photo_id")
        saveOb.appendToCassandraTableDF(savephotoDf,topic3(0))
      })

    val topic4 = Array("review") //"review","tip","user")
    val reviewStream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      LocationStrategies.PreferConsistent,
      Subscribe[String, String](topic4, kafkaParams)
    )

    val reviewMessages = reviewStream.map(ConsumerRecord => ConsumerRecord.value())
    import spark.implicits._

    reviewMessages.foreachRDD (
      reviewRdd => {
        val reviewDf = spark.read.json(reviewRdd.toDS())
//        reviewDf.printSchema()
        val saveReviewDf = reviewDf.select("business_id","caption", "label","photo_id")
        saveOb.appendToCassandraTableDF(saveReviewDf,topic4(0))
      })

    val topic5 = Array("tip") //"tip","user")
    val tipStream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      LocationStrategies.PreferConsistent,
      Subscribe[String, String](topic5, kafkaParams)
    )

    val tipMessages = tipStream.map(ConsumerRecord => ConsumerRecord.value())
    import spark.implicits._

    tipMessages.foreachRDD (
      tipRdd => {
        val tipDf = spark.read.json(tipRdd.toDS())
        tipDf.printSchema()
        val saveTipDf = tipDf.select("business_id","compliment_count", "user_id","date")
        saveOb.appendToCassandraTableDF(saveTipDf,topic5(0))
      })

    val topic6 = Array("tip") //"user")
    val userStream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      LocationStrategies.PreferConsistent,
      Subscribe[String, String](topic6, kafkaParams)
    )

    val userMessages = userStream.map(ConsumerRecord => ConsumerRecord.value())
    import spark.implicits._

    userMessages.foreachRDD (
      userRdd => {
        val userDf = spark.read.json(userRdd.toDS())
        userDf.printSchema()
        val saveUserDf = userDf.select("business_id","compliment_count", "user_id","date")
        saveOb.appendToCassandraTableDF(saveUserDf,topic6(0))
      })

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
