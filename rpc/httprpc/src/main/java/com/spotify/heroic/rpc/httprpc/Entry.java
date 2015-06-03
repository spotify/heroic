package com.spotify.heroic.rpc.httprpc;

import com.google.inject.Inject;
import com.spotify.heroic.HeroicConfigurationContext;
import com.spotify.heroic.HeroicModule;

public class Entry implements HeroicModule {
    @Inject
    private HeroicConfigurationContext config;

    @Override
    public void setup() {
        config.registerType("http", HttpRpcProtocolModule.class);
        config.resource(HttpRpcResource.class);
    }
}