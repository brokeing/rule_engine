package org.why.source;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import sun.lwawt.macosx.CPrinterDevice;

public class KafkaSource extends RichParallelSourceFunction<Map<String, Object>> {
    private boolean isStop = false;
    private KafkaConsumer<String, String> KafkaConsumer;
    private final Logger logger =  LoggerFactory.getLogger(KafkaSource.class);
    private String topic;
    private String groupId;
    public KafkaSource(String topic, String groupId){
        this.topic = topic;
        this.groupId = groupId;
    }


    @Override
    public void run(SourceContext<Map<String, Object>> sourceContext) {
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "bigdata:9092");
        prop.put("group.id", this.groupId);
        prop.put("auto.offset.reset", "latest");
        prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("enable.auto.commit", "true");
        prop.put("session.timeout.ms", "30000");
        KafkaConsumer = new KafkaConsumer<>(prop);
        ArrayList<String> topics = new ArrayList<>();
        topics.add(this.topic);
        KafkaConsumer.subscribe(topics);
        while (!isStop)
        {
            try {
                ConsumerRecords<String, String> records = KafkaConsumer.poll(1000);
                records.forEach((ConsumerRecord<String, String> record) -> {
                    String topicName = record.topic();
                    String value = record.value();
                    Map<String, Object> parse = (Map<String, Object>)JSONObject.parse(value);
                    sourceContext.collect(parse);
                });
            } catch (Exception e) {
                logger.error("kafka exception ：" +e);
            }
        }
    }

    @Override
    public void cancel() {
        isStop=true;
        KafkaConsumer.close();
    }
}
