package com.spotify.heroic.metric.astyanax;

import javax.inject.Inject;

import com.spotify.heroic.HeroicConfigurationContext;
import com.spotify.heroic.HeroicModule;

public class Entry implements HeroicModule {
    @Inject
    private HeroicConfigurationContext config;

    @Override
    public void setup() {
        config.registerType("astyanax", AstyanaxMetricModule.class);
    }
}
