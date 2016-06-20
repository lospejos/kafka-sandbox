package com.ipponusa.strings

import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConverters._

object SimpleStringConsumer extends App {

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9093")
  props.put("group.id", "mygroup")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

  val topic = "mytopic"

  val consumer = new KafkaConsumer[String, String](props)
  consumer.subscribe(topic)

  while (true) {
    val records = consumer.poll(100).get(topic)
    for (record <- records.records().asScala) {
      println(s"key: ${record.key}")
      println(s"value: ${record.value}")
    }
  }

  consumer.close()

}
