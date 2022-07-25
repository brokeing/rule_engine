package org.why.core;

import com.googlecode.aviator.Expression;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.why.config.Rule;
import org.why.config.RuleData;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class ChildProcessFunction extends BroadcastProcessFunction<Map<String, Object>, Map<String, Object>, RuleData> {
    private static final Logger logger = LoggerFactory.getLogger(ChildProcessFunction.class);
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void processElement(Map<String, Object> value, ReadOnlyContext ctx, Collector<RuleData> out) throws Exception {
        ReadOnlyBroadcastState<String, Map<Integer, Object>> broadcastState = ctx.getBroadcastState(StateDescriptors.ruleDescriptor);
        Map<Integer, Object> rules = broadcastState.get("rules");
        if (rules == null){
            // TODO无规则暂时不处理
            return;
        }
        for (Map.Entry<Integer, Object> ruleObject : rules.entrySet()) {
            Rule rule = (Rule)ruleObject.getValue();
            Expression filter = rule.getFilter();
            boolean execute = (boolean)filter.execute(value);
            if (execute){
                String keyByFields = rule.getKeyByFields();
                String key = (String)value.get(keyByFields) + String.valueOf(rule.getId());
                RuleData ruleData = new RuleData(key, value, rule);
                out.collect(ruleData);
            }
        }
    }

    @Override
    public void processBroadcastElement(Map<String, Object> rules, BroadcastProcessFunction<Map<String, Object>, Map<String, Object>, RuleData>.Context ctx, Collector<RuleData> out) throws Exception {
        BroadcastState<String, Map<Integer, Object>> broadcastState = ctx.getBroadcastState(StateDescriptors.ruleDescriptor);


        if (!broadcastState.contains("rules")){
            broadcastState.put("rules", new HashMap<>());
        }
        Map<Integer, Object> broadcastStateRules = broadcastState.get("rules");
        String type = (String)rules.get("type");
        if (type.equals("add")){
            Rule rule = new Rule();
            rule.setId((int)rules.get("id"));
            String key = (String)rules.get("key");
            rule.setKeyByFields(key);
            rule.setFilter((String)rules.get("filter"));
            Map<String, Object> window = (Map<String, Object>)rules.getOrDefault("window", null);
            rule.setWindow(window);
            broadcastStateRules.put((Integer)rules.get("id"), rule);
            log.info("加载到规则: " + rule);
        }
        if (type.equals("remove")){
            rules.remove((String) rules.get("id"));
        }

    }
}
