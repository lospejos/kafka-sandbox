package com.ipponusa.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Properties;

public class SimpleAvroProducer {

    public static final String USER_SCHEMA = "{"
            + "\"type\":\"record\","
            + "\"name\":\"myrecord\","
            + "\"fields\":["
            + "  { \"name\":\"str1\", \"type\":\"string\" },"
            + "  { \"name\":\"str2\", \"type\":\"string\" },"
            + "  { \"name\":\"int1\", \"type\":\"int\" }"
            + "]}";

    public static void main(String[] args) throws InterruptedException, IOException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(USER_SCHEMA);

        EncoderFactory avroEncoderFactory = EncoderFactory.get();
        GenericDatumWriter<GenericRecord> avroWriter = new GenericDatumWriter<>(schema);
        ByteArrayOutputStream ostream = new ByteArrayOutputStream();
        BinaryEncoder avroBinaryEncoder = avroEncoderFactory.binaryEncoder(ostream, null);

        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 50; i++) {
            GenericData.Record avroRecord = new GenericData.Record(schema);
            avroRecord.put("str1", "Str 1-" + i);
            avroRecord.put("str2", "Str 2-" + i);
            avroRecord.put("int1", i);

            avroWriter.write(avroRecord, avroBinaryEncoder);
            avroBinaryEncoder.flush();
            IOUtils.closeQuietly(ostream);
            byte[] bytes = ostream.toByteArray();
            ostream.reset();

            ProducerRecord<String, byte[]> record = new ProducerRecord<>("mytopic", bytes);
            producer.send(record);

            Thread.sleep(250);
        }

        producer.close();
    }
}
