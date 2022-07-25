package org.why.core;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.why.config.Rule;
import org.why.config.RuleData;


public class BoundedOutOfRuleWaterMaskGenerator implements WatermarkGenerator<RuleData> {
    private final long delayTime = 3500;
    private Long currentMaxTimestamp = Long.MIN_VALUE + delayTime + 1L;

    @Override
    public void onEvent(RuleData event, long eventTimestamp, WatermarkOutput output) {
        Rule rule = event.getRule();
        String keyByFields = rule.getWindow().getTimeField();
        long eventTime = (long)event.getData().get(keyByFields);
        currentMaxTimestamp = Math.max(currentMaxTimestamp, eventTime);
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        output.emitWatermark(new Watermark(currentMaxTimestamp - delayTime - 1 ));

    }
}
