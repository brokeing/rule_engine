package org.why.config;

import java.util.Map;

public class RuleData{
    private String key;
    private Map<String, Object> data;
    private Rule rule;
    private boolean isWindowRule;

    public void setKey(String key) {
        this.key = key;
    }

    public void setData(Map<String, Object> data) {
        this.data = data;
    }

    public void setRule(Rule rule) {
        this.rule = rule;
    }

    public void setWindowRule(boolean windowRule) {
        isWindowRule = windowRule;
    }

    public boolean isWindowRule() {
        return isWindowRule;
    }

    @Override
    public String toString() {
        return "RuleData{" +
                "key='" + key + '\'' +
                ", data=" + data +
                ", rule=" + rule +
                ", isWindowRule=" + isWindowRule +
                '}';
    }

    public RuleData(String key, Map<String, Object> data, Rule rule) {
        this.key = key;
        this.data = data;
        this.rule = rule;
        this.isWindowRule = (rule.getWindow() != null);
    }

    public String getKey() {
        return key + rule.getId();
    }


    public Map<String, Object> getData() {
        return data;
    }

    public Rule getRule() {
        return rule;
    }
}
