package org.why.config;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.AviatorEvaluatorInstance;
import com.googlecode.aviator.Expression;



public class WindowRule{
    private Long time;
    private String function;
    private String timeField;

    public void setTime(Long time) {
        //time 为 1 为 1分钟
        this.time = time*1000*60;
    }

    public void setFunction(String function) {
        this.function = function;
//        AviatorEvaluatorInstance instance = AviatorEvaluator.getInstance();
//        this.function = instance.compile(function);
    }

    public void setTimeField(String timeField) {
        this.timeField = timeField;
    }

    public Long getTime() {
        return time;
    }

    public String getFunction() {
        return function;
    }

    public String getTimeField() {
        return timeField;
    }

    public WindowRule(){}
}
