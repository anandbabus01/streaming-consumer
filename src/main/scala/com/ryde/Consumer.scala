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
        if ((rdd.isEmpty()) && rdd != null) {
          print("No data received in stream\n")
        }
        else {
          val df = spark.read.json(rdd.toDS())
          df.show(5)
          val saveDf = df.select("business_id", "name", "address", "city", "state", "postal_code", "stars")
          saveOb.appendToCassandraTableDF(saveDf, topics(0))
        }
      })
      streamingContext.start()
    streamingContext.awaitTermination()
  }

}
