package org.why.core;

import org.apache.flink.api.common.eventtime.*;

import org.why.config.RuleData;

import java.time.Duration;

public class SessionAssigner implements WatermarkStrategy<RuleData> {

    @Override
    public TimestampAssigner<RuleData> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return WatermarkStrategy.super.createTimestampAssigner(context);
    }

    @Override
    public WatermarkStrategy<RuleData> withTimestampAssigner(TimestampAssignerSupplier<RuleData> timestampAssigner) {
        return WatermarkStrategy.super.withTimestampAssigner(timestampAssigner);
    }

    @Override
    public WatermarkStrategy<RuleData> withTimestampAssigner(SerializableTimestampAssigner<RuleData> timestampAssigner) {
        return WatermarkStrategy.super.withTimestampAssigner(timestampAssigner);
    }

    @Override
    public WatermarkStrategy<RuleData> withIdleness(Duration idleTimeout) {
        return WatermarkStrategy.super.withIdleness(idleTimeout);
    }

    @Override
    public WatermarkGenerator<RuleData> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {

        return new BoundedOutOfRuleWaterMaskGenerator();
    }
}
