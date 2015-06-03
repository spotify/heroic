package com.spotify.heroic.aggregationcache.cassandra2;

import javax.inject.Inject;

import com.spotify.heroic.HeroicConfigurationContext;
import com.spotify.heroic.HeroicModule;

public class Entry implements HeroicModule {
    @Inject
    private HeroicConfigurationContext configurationContext;

    @Override
    public void setup() {
        configurationContext.registerType("cassandra2", Cassandra2AggregationCacheBackendModule.class);
    }
}
