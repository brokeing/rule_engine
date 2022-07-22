package org.why.core;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.why.config.Rule;
import org.why.config.RuleData;

public class BoundedOutOfRuleWaterMaskGenerator implements WatermarkGenerator<RuleData> {
    private final long maxOutOfOrderness = 3500;
    private Long currentMaxTimestamp;
    private Long firstWaterMake;
    private Long windowSize;
    @Override
    public void onEvent(RuleData event, long eventTimestamp, WatermarkOutput output) {
        Rule rule = event.getRule();
        String keyByFields = rule.getWindow().getTimeField();
        long eventTime = (long)event.getData().get(keyByFields);
        if (windowSize == null) {
            windowSize = event.getRule().getWindow().getTime();
        }
        if (firstWaterMake == null){
            firstWaterMake = System.currentTimeMillis();
        }
        currentMaxTimestamp = eventTime - maxOutOfOrderness;
        System.out.println("currentMaxTimestamp = " + currentMaxTimestamp);
        output.emitWatermark(new Watermark(currentMaxTimestamp - 1));
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        // 如果当前时间大于了窗口必须触发
        if (firstWaterMake != null || windowSize != null) {
            if (System.currentTimeMillis() - firstWaterMake > windowSize + maxOutOfOrderness){
                currentMaxTimestamp = currentMaxTimestamp + windowSize + 200;
                output.emitWatermark(new Watermark(currentMaxTimestamp));
            }
        }

    }
}
