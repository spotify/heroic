package com.spotify.heroic.statistics.semantic;

import lombok.RequiredArgsConstructor;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.spotify.heroic.statistics.ConsumerReporter;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;

@RequiredArgsConstructor
public class SemanticConsumerReporter implements ConsumerReporter {
    private final Meter messageIn;
    private final Meter messageError;
    private final Histogram messageSize;

    public SemanticConsumerReporter(SemanticMetricRegistry registry,
            String context) {
        final MetricId id = MetricId.build("consumer").tagged("context",
                context);
        messageIn = registry.meter(id.resolve("message-in"));
        messageError = registry.meter(id.resolve("message-error"));
        messageSize = registry.histogram(id.resolve("message-size"));
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
