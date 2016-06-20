package com.ipponusa.strings

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkStringConsumer extends App {

  val conf = new SparkConf()
    .setAppName("kafka-sandbox")
    .setMaster("local[*]")
  val sc = new SparkContext(conf)
  val ssc = new StreamingContext(sc, Seconds(2))

  val topics = Set("mytopic")
  val kafkaParams = Map("metadata.broker.list" -> "localhost:9093")

  val directKafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

  directKafkaStream.foreachRDD(rdd => {
    println(s"--- New RDD with ${rdd.partitions.size} partitions and ${rdd.count()} records")
    rdd.foreach(record => println(record._2))
  })

  ssc.start()
  ssc.awaitTermination()

}
