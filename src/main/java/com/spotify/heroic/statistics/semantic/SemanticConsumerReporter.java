package com.spotify.heroic.statistics.semantic;

import lombok.RequiredArgsConstructor;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.spotify.heroic.statistics.ConsumerReporter;
import com.spotify.heroic.statistics.ThreadPoolReporter;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;

@RequiredArgsConstructor
public class SemanticConsumerReporter implements ConsumerReporter {
    private static final String COMPONENT = "consumer";

    private final SemanticMetricRegistry registry;
    private final MetricId base;

    private final Meter messageIn;
    private final Meter messageError;
    private final Meter consumerSchemaError;
    private final Histogram messageSize;

    public SemanticConsumerReporter(SemanticMetricRegistry registry, String id) {
        this.registry = registry;

        this.base = MetricId.build().tagged("component", COMPONENT, "id", id);

        messageIn = registry.meter(base.tagged("what", "message-in", "unit", Units.MESSAGE));
        messageError = registry.meter(base.tagged("what", "message-error", "unit", Units.FAILURE));
        consumerSchemaError = registry.meter(base.tagged("what", "consumer-schema-error", "unit", Units.FAILURE));
        messageSize = registry.histogram(base.tagged("what", "message-size", "unit", Units.BYTE));
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

    @Override
    public void reportConsumerSchemaError() {
        consumerSchemaError.mark();
    }

    @Override
    public ThreadPoolReporter newThreadPool() {
        return new SemanticThreadPoolReporter(registry, base);
    }
}
