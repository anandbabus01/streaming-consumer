
prerequisites:
Spark 2.3 standalone or any distribution

Cassandra 3.11.2 or above

if Cassandra not listening to localhost:9042, plese update IP address in consumer.scala
line 35#
      .set("spark.cassandra.connection.host", "127.0.0.1")

if Kafka not listening to localhost:9092, please update kafka server ip in consumer.scala
line 25#
	      "bootstrap.servers" -> "localhost:9092",


before start spark stream, please run below scipts in cqlsh: ( it is in streaming-consumer repo ) 
create_cassandra_tables.cql


use this command to run spark streaming consumer:

spark-2.3.0-bin-hadoop2.7/bin/spark-submit --driver-memory 4g --executor-memory 8g --conf spark.driver.host=localhost --conf spark.cores.max=2 --master spark://localhost:7077 --class com.ryde.Consumer /home/ubuntu/yelp/streaming-consumer/target/streaming-consumer-1.0-SNAPSHOT-jar-with-dependencies.jar

