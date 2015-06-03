package com.spotify.heroic.aggregationcache;

import javax.inject.Singleton;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.spotify.heroic.statistics.AggregationCacheReporter;
import com.spotify.heroic.statistics.HeroicReporter;

public class AggregationCacheModule extends PrivateModule {
    private final AggregationCacheBackendModule backend;

    /**
     * @param backend Backend to use for caching the results of an aggregation.
     */
    @JsonCreator
    public AggregationCacheModule(@JsonProperty("backend") AggregationCacheBackendModule backend) {
        this.backend = Optional.fromNullable(backend).or(InMemoryAggregationCacheBackendConfig.defaultSupplier());
    }

    public static Supplier<AggregationCacheModule> defaultSupplier() {
        return new Supplier<AggregationCacheModule>() {
            @Override
            public AggregationCacheModule get() {
                return new AggregationCacheModule(null);
            }
        };
    }

    @Provides
    @Singleton
    public AggregationCacheReporter reporter(HeroicReporter reporter) {
        return reporter.newAggregationCache();
    }

    @Override
    protected void configure() {
        install(backend.module());
        bind(AggregationCache.class).to(AggregationCacheImpl.class).in(Scopes.SINGLETON);
        expose(AggregationCache.class);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private AggregationCacheBackendModule backend;

        public Builder backend(AggregationCacheBackendModule backend) {
            this.backend = backend;
            return this;
        }

        public AggregationCacheModule build() {
            return new AggregationCacheModule(backend);
        }
    }
}