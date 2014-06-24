package com.spotify.heroic.statistics.semantic;

import lombok.RequiredArgsConstructor;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.spotify.heroic.statistics.ConsumerReporter;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;

@RequiredArgsConstructor
public class SemanticConsumerReporter implements ConsumerReporter {
    private static final String COMPONENT = "consumer";

    private final Meter messageIn;
    private final Meter messageError;
    private final Histogram messageSize;

    public SemanticConsumerReporter(SemanticMetricRegistry registry,
            String context) {
        final MetricId id = MetricId.build().tagged("context", context,
                "component", COMPONENT);
        messageIn = registry.meter(id.tagged("what", "message-in", "unit",
                "messages"));
        messageError = registry.meter(id.tagged("what", "message-error",
                "unit", "messages"));
        messageSize = registry.histogram(id.tagged("what", "message-size",
                "unit", "messages"));
    }

    @Override
    public void reportMessageSize(int size) {
        messageIn.mark();
        messageSize.update(size);
    }

    @Override
    public void reportMessageError() {
        messageError.mark();
    }
}
