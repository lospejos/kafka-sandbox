package com.ipponusa.avro;

import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class SparkAvroConsumer {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("kafka-sandbox")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(2000));

        Set<String> topics = Collections.singleton("mytopic");
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", "localhost:9092");

        JavaPairInputDStream<String, byte[]> directKafkaStream = KafkaUtils.createDirectStream(ssc,
                String.class, byte[].class, StringDecoder.class, DefaultDecoder.class, kafkaParams, topics);

        directKafkaStream.foreachRDD(rdd -> {
            System.out.println("--- New RDD with " + rdd.partitions().size()
                    + " partitions and " + rdd.count() + " records");
            rdd.foreach(record -> displayRecord(record));
        });

        ssc.start();
        ssc.awaitTermination();
    }

    private static void displayRecord(Tuple2<String, byte[]> record) throws IOException {
        byte[] bytes = record._2;

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(SimpleAvroProducer.USER_SCHEMA);
        DecoderFactory avroDecoderFactory = DecoderFactory.get();
        BinaryDecoder avroBinaryDecoder = avroDecoderFactory.binaryDecoder(bytes, null);
        GenericData.Record avroRecord = new GenericData.Record(schema);
        GenericDatumReader<GenericData.Record> avroDatumReader = new GenericDatumReader<>(schema);
        avroRecord = avroDatumReader.read(avroRecord, avroBinaryDecoder);

        System.out.println("str1= " + avroRecord.get("str1") + ", str2= " + avroRecord.get("str2") + ", int1=" + avroRecord.get("int1"));
    }

}
