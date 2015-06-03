package com.spotify.heroic.cluster.discovery.simple;

import javax.inject.Inject;

import com.spotify.heroic.HeroicConfigurationContext;
import com.spotify.heroic.HeroicModule;

public class Entry implements HeroicModule {
    @Inject
    private HeroicConfigurationContext context;

    @Override
    public void setup() {
        context.registerType("static", StaticListDiscoveryModule.class);
    }
}
