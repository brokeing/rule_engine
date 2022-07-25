package org.why.core;

import com.googlecode.aviator.Expression;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.why.config.WindowResult;
import org.why.config.Rule;
import org.why.config.RuleData;
import org.why.config.WindowRule;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

@Slf4j
public class WindowProcessFunction extends KeyedProcessFunction<String, RuleData, Map<String, Object>> {

    private MapState<Integer, WindowResult> windowState;
    private String format = "yyyy-MM-dd HH:mm:ss";
    private SimpleDateFormat sdf = new SimpleDateFormat(format);
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        windowState = getRuntimeContext().getMapState(new MapStateDescriptor<>("windowState", Types.INT, TypeInformation.of(WindowResult.class)));
    }
    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<String, RuleData, Map<String, Object>>.OnTimerContext ctx, Collector<Map<String, Object>> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        Iterator<Map.Entry<Integer, WindowResult>> iterator = windowState.entries().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Integer, WindowResult> entry = iterator.next();
            log.info("触发定时器，规则id: " + entry.getValue().getRuleId() +
                    " 当前结束时间为: " + sdf.format(new Date(timestamp)));
            if ((entry.getValue().getEndTime()/1000)*1000 == timestamp) {
                out.collect(entry.getValue().getResult());
                ctx.timerService().deleteEventTimeTimer(timestamp);
                iterator.remove();
            }
        }
        Iterator<Map.Entry<Integer, WindowResult>> tmpIterator = windowState.entries().iterator();
        StringBuilder sb = new StringBuilder();
        while (tmpIterator.hasNext()){
            Map.Entry<Integer, WindowResult> next = tmpIterator.next();
            Integer key = next.getKey();
            sb.append(": ").append(key);
        }
        log.info("剩余规则id为: " + sb.toString());
    }

    @Override
    public void processElement(RuleData value, KeyedProcessFunction<String, RuleData, Map<String, Object>>.Context ctx, Collector<Map<String, Object>> out) throws Exception {
        Map<String, Object> data = value.getData();
        Rule rule = value.getRule();
        int id = rule.getId();
        WindowRule window = rule.getWindow();
        if (window == null){
            WindowResult outPutRule = new WindowResult(data, id, value.getKey(), System.currentTimeMillis(), System.currentTimeMillis());
            out.collect(outPutRule.getResult());
        } else {
            WindowResult outPutRuleCache = windowState.get(id);
            long triggerTime = (Long) data.get(window.getTimeField()) + window.getTime();
            if (outPutRuleCache == null) {
                ctx.timerService().registerEventTimeTimer((triggerTime/1000)*1000);
                long currentWatermark = ctx.timerService().currentWatermark();
                log.info("当前水位线是:" + sdf.format(new Date(currentWatermark)) +
                        " 当前触发时间为: " +  sdf.format(new Date(triggerTime)) +
                        " 当前系统时间为: " + sdf.format(new Date(System.currentTimeMillis())));
                WindowResult outPutRule = new WindowResult(
                        id,
                        value.getKey(),
                        (Long) data.get(window.getTimeField()),
                        triggerTime,
                        window.getFunction(),
                        rule.getWindow().getTime());
                Expression addFunction = outPutRule.getAddFunction();
                Map<String, Object> resultAndData = formatResultAndData(null, data);
                Map<String, Object> execute = (Map<String, Object>)addFunction.execute(resultAndData);
                outPutRule.setData(execute);
                windowState.put(id, outPutRule);
            } else {
                Expression addFunction = outPutRuleCache.getAddFunction();
                Map<String, Object> resultData = outPutRuleCache.getData();
                Map<String, Object> resultAndData = formatResultAndData(resultData, data);
                Map<String, Object> execute = (Map<String, Object>)addFunction.execute(resultAndData);
                outPutRuleCache.setData(execute);
            }
        }
    }

    private Map<String, Object> formatResultAndData(Map<String, Object> result, Map<String, Object> data) {
        Map<String, Object> formatData = new HashMap<>();
        if (result == null){
            result = new HashMap<>();
        }
        formatData.put("result", result);
        formatData.put("data", data);
        return formatData;
    }
}
