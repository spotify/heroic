package com.spotify.heroic.metric.cassandra2;

import com.spotify.heroic.HeroicContext;
import com.spotify.heroic.HeroicModuleEntryPoint;

public class EntryPoint implements HeroicModuleEntryPoint {
    @Override
    public void setup(HeroicContext context) {
        context.register("cassandra2", Cassandra2MetricModule.class);
    }
}
