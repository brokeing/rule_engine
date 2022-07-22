package org.why.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class RuleSource implements SourceFunction<String> {
    @Override
    public void run(SourceContext<String> ctx) throws Exception {

    }

    @Override
    public void cancel() {

    }
}
