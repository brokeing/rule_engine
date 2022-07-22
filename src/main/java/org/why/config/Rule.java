package org.why.config;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;

import java.util.Map;

public class Rule {
    private int id;
    private transient Expression filter;
    private WindowRule window;

    @Override
    public String toString() {
        return "Rule{" +
                "id=" + id +
                ", filter=" + filter +
                ", window=" + window +
                ", keyByFields='" + keyByFields + '\'' +
                '}';
    }

    private String keyByFields;

    public void setId(int id) {
        this.id = id;
    }

    public void setFilter(String filter) {
        this.filter = AviatorEvaluator.compile(filter);
    }

    public void setWindow(Map<String, Object> window) {
        if (window == null){
            this.window = null;
        } else {
            Long time = new Long((int)window.get("time"));
            String function = (String)window.get("function");
            String time_filed = (String)window.get("time_field");
            this.window = new WindowRule();
            this.window.setTime(time);
            this.window.setTimeField(time_filed);
            this.window.setFunction(function);
        }
    }

    public void setKeyByFields(String keyByFields) {
        this.keyByFields = keyByFields;
    }

    public int getId() {
        return id;
    }

    public Expression getFilter() {
        return filter;
    }

    public WindowRule getWindow() {
        return window;
    }

    public String getKeyByFields() {
        return keyByFields;
    }

    public Rule(){}
}
