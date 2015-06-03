package com.spotify.heroic.metric;

import com.google.inject.Key;
import com.google.inject.Module;

public interface MetricModule {
    public Module module(Key<MetricBackend> key, String id);

    public String id();

    public String buildId(int i);
}
