package com.ipponusa.strings

import java.util.Properties

import io.confluent.kafka.serializers.KafkaAvroDecoder
import kafka.consumer.ConsumerConfig
import kafka.utils.VerifiableProperties
import org.apache.avro.generic.GenericRecord

import scala.collection.JavaConverters._

object Consumer extends App {

  val props = new Properties()
  props.put("zookeeper.connect", "localhost:2182")
  props.put("group.id", "group1")
  props.put("schema.registry.url", "http://localhost:8091")

  val topic = "Kafka"
  val topicCountMap = Map(topic -> 1)

  val vProps = new VerifiableProperties(props)
  val keyDecoder = new KafkaAvroDecoder(vProps)
  val valueDecoder = new KafkaAvroDecoder(vProps)

  val consumer = kafka.consumer.Consumer.create(new ConsumerConfig(props))

  val consumerMap = consumer.createMessageStreams(topicCountMap, keyDecoder, valueDecoder)
  val stream = consumerMap.get(topic).get(0)
  for (messageAndMetadata <- stream.asScala) {
    val key = messageAndMetadata.key.asInstanceOf[String]
    val value = messageAndMetadata.message.asInstanceOf[GenericRecord]
    val f1Idx = value.getSchema().getField("f1").pos()
    println(s"timestamp: ${messageAndMetadata.timestamp}")
    println(s"f1: ${value.get(f1Idx)}")
  }
}
