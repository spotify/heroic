package com.spotify.heroic.metadata.lucene;

import javax.inject.Inject;

import com.spotify.heroic.ConfigurationContext;
import com.spotify.heroic.HeroicEntryPoint;

public class EntryPoint implements HeroicEntryPoint {
    @Inject
    private ConfigurationContext context;

    @Override
    public void setup() {
        context.registerType("lucene", LuceneMetadataModule.class);
    }
}
