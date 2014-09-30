package com.spotify.heroic.statistics.semantic;

import com.spotify.heroic.statistics.HttpClientManagerReporter;
import com.spotify.heroic.statistics.ThreadPoolReporter;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;

public class SemanticHttpClientManagerReporter implements
HttpClientManagerReporter {
    private static final String COMPONENT = "http-client-manager";

    private final SemanticMetricRegistry registry;
    private final MetricId id;

    public SemanticHttpClientManagerReporter(SemanticMetricRegistry registry) {
        this.id = MetricId.build().tagged("component", COMPONENT);
        this.registry = registry;
    }

    @Override
    public ThreadPoolReporter newThreadPool() {
        return new SemanticThreadPoolReporter(registry, id);
    }
}
