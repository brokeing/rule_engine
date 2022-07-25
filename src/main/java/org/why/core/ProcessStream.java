package org.why.core;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.why.config.RuleData;
import org.why.source.KafkaSource;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;

public class ProcessStream {

    private StreamExecutionEnvironment env;
    public ProcessStream() {
        // TODO flink config
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
    }
    public ProcessStream build(){
        DataStream<Map<String, Object>> dataStream =
                env.addSource(new KafkaSource("why_data_topic", "g2"));
        DataStreamSource<Map<String, Object>> ruleDataStream =
                env.addSource(new KafkaSource("why_rule_topic", "g2"));
        BroadcastStream<Map<String, Object>> broadcast = ruleDataStream.broadcast(StateDescriptors.ruleDescriptor);

        SingleOutputStreamOperator<Map<String, Object>> process = dataStream.connect(broadcast)
                .process(new ChildProcessFunction())
                .assignTimestampsAndWatermarks(new SessionAssigner())
                .keyBy(RuleData::getKey)
                .process(new WindowProcessFunction());
        process.print();

        // TODO sink
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "bigdata:9092");
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>("why_rule_result_topic",
                new SimpleStringSchema(),
                properties);
        return this;
    }
    public void start() throws Exception {
        this.env.execute();
    };
}
