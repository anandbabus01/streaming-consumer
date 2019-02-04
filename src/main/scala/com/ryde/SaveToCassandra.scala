package com.ryde

import java.util
import java.util.{HashMap, Map}

import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.collection.mutable

class SaveToCassandra {
  def appendToCassandraTableDF(df : DataFrame)={
    val options = new util.HashMap[String, String]
    options.put("keyspace", "test".toLowerCase)
    options.put("table", "details")

    try {
      df.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(options).save()
    }
    catch {
      case e: Exception =>
        throw e
    } finally
      df.unpersist

  }

}
