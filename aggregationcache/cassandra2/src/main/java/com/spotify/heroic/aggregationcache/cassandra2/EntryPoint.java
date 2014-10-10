package com.spotify.heroic.aggregationcache.cassandra2;

import javax.inject.Inject;

import com.spotify.heroic.ConfigurationContext;
import com.spotify.heroic.HeroicEntryPoint;

public class EntryPoint implements HeroicEntryPoint {
    @Inject
    private ConfigurationContext configurationContext;

    @Override
    public void setup() {
        configurationContext.registerType("cassandra2", Cassandra2AggregationCacheBackendModule.class);
    }
}
