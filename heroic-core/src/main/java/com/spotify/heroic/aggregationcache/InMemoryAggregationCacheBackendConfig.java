package com.spotify.heroic.aggregationcache;

import javax.inject.Singleton;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.Supplier;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.spotify.heroic.statistics.AggregationCacheBackendReporter;
import com.spotify.heroic.statistics.AggregationCacheReporter;

public class InMemoryAggregationCacheBackendConfig implements AggregationCacheBackendModule {
    @JsonCreator
    public static Supplier<InMemoryAggregationCacheBackendConfig> defaultSupplier() {
        return new Supplier<InMemoryAggregationCacheBackendConfig>() {
            @Override
            public InMemoryAggregationCacheBackendConfig get() {
                return new InMemoryAggregationCacheBackendConfig();
            }
        };
    }

    @Override
    public Module module() {
        return new PrivateModule() {
            @Provides
            @Singleton
            public AggregationCacheBackendReporter reporter(AggregationCacheReporter reporter) {
                return reporter.newAggregationCacheBackend();
            }

            @Override
            protected void configure() {
                bind(AggregationCacheBackend.class).to(InMemoryAggregationCacheBackend.class).in(Scopes.SINGLETON);
                expose(AggregationCacheBackend.class);
            }
        };
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        public AggregationCacheBackendModule build() {
            return new InMemoryAggregationCacheBackendConfig();
        }
    }
}