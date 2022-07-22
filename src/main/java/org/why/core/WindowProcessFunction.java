package org.why.core;

import com.googlecode.aviator.Expression;
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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class WindowProcessFunction extends KeyedProcessFunction<String, RuleData, Map<String, Object>> {
    private static final Logger logger = LoggerFactory.getLogger(WindowProcessFunction.class);

    private MapState<Integer, WindowResult> windowState;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        windowState = getRuntimeContext().getMapState(new MapStateDescriptor<>("windowState", Types.INT, TypeInformation.of(WindowResult.class)));
    }
    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<String, RuleData, Map<String, Object>>.OnTimerContext ctx, Collector<Map<String, Object>> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        System.out.println("触发onTime = ");
        Iterator<Map.Entry<Integer, WindowResult>> iterator = windowState.entries().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Integer, WindowResult> entry = iterator.next();
            logger.info(entry.getValue().getEndTime().toString());
            if (entry.getValue().getEndTime() == timestamp) {
                out.collect(entry.getValue().getResult());
//                ctx.timerService().deleteEventTimeTimer(timestamp);
//                ctx.timerService().deleteProcessingTimeTimer(timestamp);
                iterator.remove();
            }
        }
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
//                ctx.timerService().registerProcessingTimeTimer(triggerTime);
                ctx.timerService().registerEventTimeTimer(triggerTime);
                long currentWatermark = ctx.timerService().currentWatermark();
                logger.info(currentWatermark + "/" + triggerTime + "/" + System.currentTimeMillis());
                WindowResult outPutRule = new WindowResult(id, value.getKey(), (Long) data.get(window.getTimeField()), triggerTime,window.getFunction());
                Expression addFunction = outPutRule.getAddFunction();
                Map<String, Object> resultAndData = formatResultAndData(null, data);
                Map<String, Object> execute = (Map<String, Object>)addFunction.execute(resultAndData);
                outPutRule.setData(execute);
                windowState.put(id, outPutRule);
            } else {
                Expression addFunction = outPutRuleCache.getAddFunction();
                logger.info("当前水位线是:" + ctx.timerService().currentWatermark() + " 当前触发时间为: " + triggerTime + " 当前系统时间为: " + System.currentTimeMillis());
                Map<String, Object> resultData = outPutRuleCache.getData();
                Map<String, Object> resultAndData = formatResultAndData(resultData, data);
                Map<String, Object> execute = (Map<String, Object>)addFunction.execute(resultAndData);
                outPutRuleCache.setEndTime((Long) data.get(window.getTimeField()));
                outPutRuleCache.setData(execute);
                logger.info("当前缓存内容为：" + windowState.get(id));
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
