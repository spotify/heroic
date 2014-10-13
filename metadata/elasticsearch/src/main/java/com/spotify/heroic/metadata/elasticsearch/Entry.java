package com.spotify.heroic.metadata.elasticsearch;

import javax.inject.Inject;

import com.spotify.heroic.ConfigurationContext;
import com.spotify.heroic.HeroicEntryPoint;

public class Entry implements HeroicEntryPoint {
    @Inject
    private ConfigurationContext context;

    @Override
    public void setup() {
        context.registerType("elasticsearch", ElasticSearchMetadataModule.class);
    }
}
