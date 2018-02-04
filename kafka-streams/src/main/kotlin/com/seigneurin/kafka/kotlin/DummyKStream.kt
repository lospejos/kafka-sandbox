package com.seigneurin.kafka.kotlin

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.Consumed
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Produced
import java.util.*

/*
 * Transforms values from 'text-input' to 'text-output':
 * - only keeps text containing the letter 'a'
 * - capitalizes the text
 *
 * Before running this:
 * - kafka-topics --zookeeper localhost:2181 --create --topic text-input --partitions 1 --replication-factor 1
 * - kafka-topics --zookeeper localhost:2181 --create --topic text-output --partitions 1 --replication-factor 1
 *
 * Launch a consumer to display the output:
 * - kafka-console-consumer --zookeeper localhost:2181 --topic text-output
 *
 * Launch a producer and type some text:
 * - kafka-console-producer --broker-list localhost:9092 --topic text-input
 */
fun main(args: Array<String>) {
    val props = Properties()
    props[StreamsConfig.APPLICATION_ID_CONFIG] = "text-transformer"
    props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
    props[StreamsConfig.COMMIT_INTERVAL_MS_CONFIG] = 5 * 1000
    props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"

    val builder = StreamsBuilder()
    builder.stream("text-input", Consumed.with(Serdes.String(), Serdes.String()))
            .filter { key, value -> value.contains("a") }
            .mapValues { it.toUpperCase() }
            .to("text-output", Produced.with(Serdes.String(), Serdes.String()))

    val streams = KafkaStreams(builder.build(), props)
    streams.start()
}
