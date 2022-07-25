package org.why;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class SendKafkaData {
    public static void main(String[] args) throws InterruptedException {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers","bigdata:9092");
        kafkaProps.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer") ;
        KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProps);

        for (int i = 0; i < 300; i++) {
            Map<String, Object> data = new HashMap<>();
            data.put("src_ip", "1.1.1.1");
            data.put("dst_ip", "2.2.2.2");
            data.put("city", "中国");
            data.put("rule_name", "暴力破解");
            data.put("equipment", "chaitin");
            data.put("time", System.currentTimeMillis());
            String log = JSON.toJSONString(data);
            ProducerRecord<String, String> record = new ProducerRecord<>("why_data_topic", log);
            producer.send(record);
            producer.flush();
            Thread.sleep(1000);
        }
    }
}
