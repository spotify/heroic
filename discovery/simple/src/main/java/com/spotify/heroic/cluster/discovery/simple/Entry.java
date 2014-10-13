package com.spotify.heroic.cluster.discovery.simple;

import javax.inject.Inject;

import com.spotify.heroic.ConfigurationContext;
import com.spotify.heroic.HeroicEntryPoint;

public class Entry implements HeroicEntryPoint {
    @Inject
    private ConfigurationContext context;

    @Override
    public void setup() {
        context.registerType("static", StaticListDiscoveryModule.class);
    }
}
