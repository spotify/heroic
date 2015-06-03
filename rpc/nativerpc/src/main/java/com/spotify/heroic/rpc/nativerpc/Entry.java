package com.spotify.heroic.rpc.nativerpc;

import com.google.inject.Inject;
import com.spotify.heroic.HeroicConfigurationContext;
import com.spotify.heroic.HeroicModule;

public class Entry implements HeroicModule {
    @Inject
    private HeroicConfigurationContext config;

    @Override
    public void setup() {
        config.registerType("nativerpc", NativeRpcProtocolModule.class);
    }
}
