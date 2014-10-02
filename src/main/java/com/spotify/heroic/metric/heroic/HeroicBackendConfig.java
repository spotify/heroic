package com.spotify.heroic.metric.heroic;

import javax.inject.Named;
import javax.inject.Singleton;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.Key;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.spotify.heroic.concurrrency.ReadWriteThreadPools;
import com.spotify.heroic.metric.MetricBackend;
import com.spotify.heroic.metric.MetricBackendConfig;
import com.spotify.heroic.statistics.MetricBackendManagerReporter;
import com.spotify.heroic.statistics.MetricBackendReporter;

@Data
public final class HeroicBackendConfig implements MetricBackendConfig {
    public static final String DEFAULT_SEEDS = "localhost:9160";
    public static final String DEFAULT_KEYSPACE = "heroic";
    public static final String DEFAULT_GROUP = "heroic";
    public static final int DEFAULT_MAX_CONNECTIONS_PER_HOST = 50;

    private final String id;
    private final String group;
    private final String keyspace;
    private final String seeds;
    private final int maxConnectionsPerHost;
    private final ReadWriteThreadPools.Config pools;

    @JsonCreator
    public static HeroicBackendConfig create(@JsonProperty("id") String id, @JsonProperty("seeds") String seeds,
            @JsonProperty("keyspace") String keyspace,
            @JsonProperty("maxConnectionsPerHost") Integer maxConnectionsPerHost, @JsonProperty("group") String group,
            @JsonProperty("pools") ReadWriteThreadPools.Config pools) {
        if (seeds == null)
            seeds = DEFAULT_SEEDS;

        if (keyspace == null)
            keyspace = DEFAULT_KEYSPACE;

        if (maxConnectionsPerHost == null)
            maxConnectionsPerHost = DEFAULT_MAX_CONNECTIONS_PER_HOST;

        if (group == null)
            group = DEFAULT_GROUP;

        if (pools == null)
            pools = ReadWriteThreadPools.Config.createDefault();

        return new HeroicBackendConfig(id, group, keyspace, seeds, maxConnectionsPerHost, pools);
    }

    @Override
    public PrivateModule module(final Key<MetricBackend> key, final String id) {
        return new PrivateModule() {
            @Provides
            @Singleton
            public MetricBackendReporter reporter(MetricBackendManagerReporter reporter) {
                return reporter.newBackend(id);
            }

            @Provides
            @Singleton
            public ReadWriteThreadPools pools(MetricBackendReporter reporter) {
                return pools.construct(reporter.newThreadPool());
            }

            @Provides
            @Singleton
            @Named("maxConnectionsPerHost")
            public int maxConnectionsPerHost() {
                return maxConnectionsPerHost;
            }

            @Provides
            @Singleton
            @Named("seeds")
            public String seeds() {
                return seeds;
            }

            @Provides
            @Singleton
            @Named("keyspace")
            public String keyspace() {
                return keyspace;
            }

            @Provides
            @Singleton
            @Named("group")
            public String group() {
                return group;
            }

            @Override
            protected void configure() {
                bind(key).to(HeroicBackend.class).in(Scopes.SINGLETON);
                expose(key);
            }
        };
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public String buildId(int i) {
        return String.format("heroic#%d", i);
    }
}
