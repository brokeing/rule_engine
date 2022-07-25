package org.why;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class SendKafkaRule {
    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers","bigdata:9092");
        kafkaProps.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer") ;
        KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProps);

        Map<String, Object> data = new HashMap<>();
        data.put("type", "add");
        data.put("id", 2);
        data.put("filter", "equipment=='chaitin'");
        Map<String, Object> window = new HashMap<>();
        window.put("time_field", "time");
        window.put("time", 3);
        window.put("function", "use java.util.HashMap; if (data.rule_name == \"暴力破解\") {if (result.count != nil) {result.count = result.count + 1; } else {result.count = 1; } } return result;");
        data.put("key", "src_ip");
        data.put("window", window);
        String log = JSON.toJSONString(data);
        ProducerRecord<String, String> record = new ProducerRecord<>("why_rule_topic", log);
        producer.send(record);
        producer.flush();
    }

}
