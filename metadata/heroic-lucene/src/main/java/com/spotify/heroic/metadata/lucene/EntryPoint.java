package com.spotify.heroic.metadata.lucene;

import com.spotify.heroic.HeroicContext;
import com.spotify.heroic.HeroicModuleEntryPoint;

public class EntryPoint implements HeroicModuleEntryPoint {
    @Override
    public void setup(HeroicContext context) {
        context.register("lucene", LuceneMetadataModule.class);
    }
}
