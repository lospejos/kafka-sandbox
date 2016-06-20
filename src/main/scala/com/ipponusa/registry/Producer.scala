package com.ipponusa.registry

import java.util.Properties

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

object Producer extends App {

  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[io.confluent.kafka.serializers.KafkaAvroSerializer])
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[io.confluent.kafka.serializers.KafkaAvroSerializer])
  props.put("schema.registry.url", "http://localhost:8091")

  val producer = new KafkaProducer[Object, Object](props)

  val key = "key1"
  val userSchema = """{"type":"record","name":"myrecord","fields":[{"name":"f1","type":"string"}]}"""
  val parser = new Schema.Parser()
  val schema = parser.parse(userSchema)

  val avroRecord = new GenericData.Record(schema)
  avroRecord.put("f1", "value1")

  val record = new ProducerRecord[Object, Object]("Kafka", key, avroRecord)
  val future = producer.send(record)
  future.get()

}
