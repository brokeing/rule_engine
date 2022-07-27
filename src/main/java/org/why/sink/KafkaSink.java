package org.why.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class KafkaSink {
    public static FlinkKafkaProducer<String> getKafkaSink(String topic){
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "bigdata:9092");
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>(topic,
                new SimpleStringSchema(),
                properties);
        return producer;
    }
}