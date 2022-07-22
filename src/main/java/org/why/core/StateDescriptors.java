package org.why.core;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.util.OutputTag;
import org.why.config.RuleData;

import java.util.Map;

public class StateDescriptors {
    public static  MapStateDescriptor<String, Map<Integer, Object>> ruleDescriptor = new MapStateDescriptor<>("rules", Types.STRING, Types.MAP(Types.INT, TypeInformation.of(Object.class)));
//    public static final OutputTag<RuleData> windowRule = new OutputTag<RuleData>("windowRule");
//    private static final OutputTag<RuleData> streamRule = new OutputTag<RuleData>("streamRule");

}
