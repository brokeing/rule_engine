package org.why.config;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.AviatorEvaluatorInstance;
import com.googlecode.aviator.Expression;

import java.util.HashMap;
import java.util.Map;

public class WindowResult {
    private Map<String, Object> data;
    private int ruleId;
    private String key;
    private Long StartTime;
    private Long endTime;
    private Expression addFunction;
    private Long windowSize;

    public void setWindowSize(Long windowSize) {
        this.windowSize = windowSize;
    }

    public Long getWindowSize() {
        return windowSize;
    }

    public void setAddFunction(Expression addFunction) {
        this.addFunction = addFunction;
    }

    public Expression getAddFunction() {
        return addFunction;
    }

    public void setData(Map<String, Object> data) {
        this.data = data;
    }

    public void setRuleId(int ruleId) {
        this.ruleId = ruleId;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setStartTime(Long startTime) {
        StartTime = startTime;
    }

    public void setEndTime(Long endTime) {
        this.endTime = endTime;
    }

    public Map<String, Object> getData() {
        return data;
    }

    public int getRuleId() {
        return ruleId;
    }

    public String getKey() {
        return key;
    }

    public Long getStartTime() {
        return StartTime;
    }

    public Long getEndTime() {
        return endTime;
    }
    public WindowResult(Map<String, Object> data, int ruleId, String key, Long startTime, Long endTime){
        this.ruleId = ruleId;
        this.key = key;
        this.StartTime = startTime;
        this.endTime = endTime;
        this.data = data;

    }

    public WindowResult(int ruleId, String key, Long startTime, Long endTime, String addFunction, Long windowSize) {
        this.data = new HashMap<>();
        this.ruleId = ruleId;
        this.key = key;
        this.StartTime = startTime;
        AviatorEvaluatorInstance instance = AviatorEvaluator.getInstance();
        this.addFunction = instance.compile(addFunction);
        this.endTime = endTime;
        this.windowSize = windowSize;
    }
    public Map<String, Object> getResult(){
        Map<String, Object> resultData = new HashMap<>();
        resultData.put("data", this.data);
        resultData.put("key", this.key);
        resultData.put("startTime", this.getStartTime());
        resultData.put("endTime", this.getEndTime());
        resultData.put("ruleId", this.ruleId);
        return resultData;
    }

    @Override
    public String toString() {
        return "WindowResult{" +
                "data=" + data +
                ", ruleId=" + ruleId +
                ", key='" + key + '\'' +
                ", StartTime=" + StartTime +
                ", endTime=" + endTime +
                ", addFunction=" + addFunction +
                '}';
    }
}
