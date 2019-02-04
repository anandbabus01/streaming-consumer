package com.ryde

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession, functions}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import com.ryde.SaveToCassandra

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
    val topics = Array("sample")

    val sparkConf = new SparkConf().setMaster("local[2]")
                    .setAppName("StreamingConsumer")
                    .set("spark.cassandra.connection.host", "127.0.0.1")
//                    .options("spark.driver.allowMultipleContexts", true)

    val spark= SparkSession.builder
      .appName("StructuredStream")
      .master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext
    val streamingContext = new StreamingContext(sc, Seconds(2))

    val dStream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      LocationStrategies.PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val messages = dStream.map(ConsumerRecord => ConsumerRecord.value())
    import spark.implicits._

    messages.foreachRDD (
      rdd => {
//        rdd.foreach(println)
//        rdd.toDF().printSchema()
//        rdd.toDF().show(2)
        val df = spark.sqlContext.read.json(rdd.toDS())
        df.show(5)
        df.printSchema()
        val busDF = df.select("name","business_id", "address")

        val saveOb = new SaveToCassandra
        saveOb.appendToCassandraTableDF(busDF)

      })

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
