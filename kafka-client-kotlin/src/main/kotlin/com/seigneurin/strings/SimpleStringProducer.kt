package com.seigneurin.strings

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties

fun main(args: Array<String>) {
    val props = Properties()
    props["bootstrap.servers"] = "localhost:9092"
    props["key.serializer"] = StringSerializer::class.java.canonicalName
    props["value.serializer"] = StringSerializer::class.java.canonicalName

    val producer = KafkaProducer<String, String>(props)
    for (i in 1..1000) {
        val record = ProducerRecord<String, String>("mytopic", "value-$i")
        producer.send(record)
        Thread.sleep(250)
    }

    producer.close()
}
