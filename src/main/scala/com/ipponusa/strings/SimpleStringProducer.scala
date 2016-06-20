package com.ipponusa.strings

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object SimpleStringProducer extends App {

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9093")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val topic = "mytopic"

  val producer = new KafkaProducer[String, String](props)
  for (i <- 0 to 1000) {
    val record = new ProducerRecord[String, String](topic, "value-" + i)
    producer.send(record)
    Thread.sleep(250)
  }

  producer.close()

}
