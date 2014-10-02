package com.spotify.heroic.aggregationcache;

import javax.inject.Singleton;

import lombok.RequiredArgsConstructor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.spotify.heroic.statistics.AggregationCacheReporter;
import com.spotify.heroic.statistics.HeroicReporter;

@RequiredArgsConstructor
public class AggregationCacheModule extends PrivateModule {
    private final AggregationCacheBackendConfig backend;

    @JsonCreator
    public static AggregationCacheModule create(@JsonProperty("backend") AggregationCacheBackendConfig backend) {
        if (backend == null)
            backend = InMemoryAggregationCacheBackendConfig.create();

        return new AggregationCacheModule(backend);
    }

    @Provides
    @Singleton
    public AggregationCacheReporter reporter(HeroicReporter reporter) {
        return reporter.newAggregationCache();
    }

    @Override
    protected void configure() {
        install(backend.module());
        bind(AggregationCache.class).in(Scopes.SINGLETON);
        expose(AggregationCache.class);
    }

    public static AggregationCacheModule createDefault() {
        return create(null);
    }
}
