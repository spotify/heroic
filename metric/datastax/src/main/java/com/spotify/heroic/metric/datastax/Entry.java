package com.spotify.heroic.metric.datastax;

import javax.inject.Inject;

import com.spotify.heroic.HeroicConfigurationContext;
import com.spotify.heroic.HeroicModule;

public class Entry implements HeroicModule {
    @Inject
    private HeroicConfigurationContext config;

    @Override
    public void setup() {
        config.registerType("datastax", DatastaxMetricModule.class);
    }
}
