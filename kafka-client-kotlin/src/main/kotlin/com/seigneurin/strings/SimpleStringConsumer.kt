package com.seigneurin.strings

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import java.util.Properties

fun main(args: Array<String>) {
    val props = Properties()
    props["bootstrap.servers"] = "localhost:9092"
    props["group.id"] = "mygroup"
    props["key.deserializer"] = StringDeserializer::class.java.canonicalName
    props["value.deserializer"] = StringDeserializer::class.java.canonicalName

    val consumer = KafkaConsumer<String, String>(props)
    consumer.subscribe(listOf("mytopic"))

    val running = true
    while (running) {
        val records = consumer.poll(100)
        for (record in records) {
            println(record.value())
        }
    }

    consumer.close()
}
