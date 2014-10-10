package com.spotify.heroic.metadata.elasticsearch;

import com.spotify.heroic.HeroicContext;
import com.spotify.heroic.HeroicModuleEntryPoint;

public class EntryPoint implements HeroicModuleEntryPoint {
    @Override
    public void setup(HeroicContext context) {
        context.register("elasticsearch", ElasticSearchMetadataModule.class);
    }
}
