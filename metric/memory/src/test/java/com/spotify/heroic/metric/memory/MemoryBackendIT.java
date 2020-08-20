package com.spotify.heroic.metric.memory;

import com.spotify.heroic.metric.MetricModule;
import com.spotify.heroic.test.AbstractMetricBackendIT;

import java.util.Optional;

public class MemoryBackendIT extends AbstractMetricBackendIT {
    @Override
    protected void setupSupport() {
        super.setupSupport();

        this.eventSupport = true;
        this.hugeRowKey = false;
    }

    @Override
    protected MetricModule setupModule() {
        return MemoryMetricModule.builder().build();
    }
}
