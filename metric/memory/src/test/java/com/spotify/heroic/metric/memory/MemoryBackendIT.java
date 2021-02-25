package com.spotify.heroic.metric.memory;

import com.spotify.heroic.metric.MetricModule;
import com.spotify.heroic.test.AbstractMetricBackendIT;

public class MemoryBackendIT extends AbstractMetricBackendIT {
    @Override
    protected void setupSupport() {
        super.setupSupport();

        this.eventSupport = true;
        this.hugeRowKey = false;
    }

    @Override
    protected MetricModule setupModule(BackendModuleMode mode) {
        return MemoryMetricModule.builder().build();
    }
}
