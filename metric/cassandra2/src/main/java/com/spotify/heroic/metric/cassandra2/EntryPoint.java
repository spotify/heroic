package com.spotify.heroic.metric.cassandra2;

import javax.inject.Inject;

import com.spotify.heroic.ConfigurationContext;
import com.spotify.heroic.HeroicEntryPoint;

public class EntryPoint implements HeroicEntryPoint {
    @Inject
    private ConfigurationContext config;

    @Override
    public void setup() {
        config.registerType("cassandra2", Cassandra2MetricModule.class);
    }
}
