package com.spotify.heroic.metadata.elasticsearch;

import javax.inject.Inject;

import com.spotify.heroic.HeroicConfigurationContext;
import com.spotify.heroic.HeroicModule;

public class Entry implements HeroicModule {
    @Inject
    private HeroicConfigurationContext context;

    @Override
    public void setup() {
        context.registerType("elasticsearch", ElasticsearchMetadataModule.class);
    }
}
