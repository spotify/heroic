package com.spotify.heroic.aggregationcache.cassandra;

import javax.inject.Singleton;

import lombok.RequiredArgsConstructor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.netflix.astyanax.AstyanaxConfiguration;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.spotify.heroic.aggregationcache.AggregationCacheBackend;
import com.spotify.heroic.aggregationcache.AggregationCacheBackendConfig;
import com.spotify.heroic.concurrrency.ReadWriteThreadPools;
import com.spotify.heroic.statistics.AggregationCacheBackendReporter;
import com.spotify.heroic.statistics.AggregationCacheReporter;

@RequiredArgsConstructor
public class CassandraCacheConfig implements AggregationCacheBackendConfig {
    public static final int DEFAULT_THREADS = 20;
    public static final String DEFAULT_KEYSPACE = "aggregations";
    public static final int DEFAULT_MAX_CONNECTIONS_PER_HOST = 20;

    private final String seeds;
    private final String keyspace;
    private final int maxConnectionsPerHost;
    private final ReadWriteThreadPools.Config pools;

    @JsonCreator
    public static CassandraCacheConfig create(
            @JsonProperty("seeds") String seeds,
            @JsonProperty("keyspace") String keyspace,
            @JsonProperty("maxConnectionsPerHost") Integer maxConnectionsPerHost,
            @JsonProperty("pools") ReadWriteThreadPools.Config pools) {
        if (seeds == null)
            throw new RuntimeException("'seeds' must be defined");

        if (keyspace == null)
            keyspace = DEFAULT_KEYSPACE;

        if (maxConnectionsPerHost == null)
            maxConnectionsPerHost = DEFAULT_MAX_CONNECTIONS_PER_HOST;

        if (pools == null)
            pools = ReadWriteThreadPools.Config.createDefault();

        return new CassandraCacheConfig(seeds, keyspace, maxConnectionsPerHost,
                pools);
    }

    @Override
    public Module module() {
        return new PrivateModule() {
            @Provides
            @Singleton
            public AggregationCacheBackendReporter reporter(
                    AggregationCacheReporter reporter) {
                return reporter.newAggregationCacheBackend();
            }

            @Provides
            @Singleton
            public ReadWriteThreadPools pools(
                    AggregationCacheBackendReporter reporter) {
                return pools.construct(reporter.newThreadPool());
            }

            @Provides
            @Singleton
            public AstyanaxContext<Keyspace> keyspace() {
                final AstyanaxConfiguration config = new AstyanaxConfigurationImpl()
                        .setCqlVersion("3.0.0")
                        .setTargetCassandraVersion("2.0");

                final AstyanaxContext<Keyspace> ctx = new AstyanaxContext.Builder()
                        .withConnectionPoolConfiguration(
                                new ConnectionPoolConfigurationImpl(
                                        "HeroicConnectionPool")
                                        .setPort(9160)
                                        .setMaxConnsPerHost(
                                                maxConnectionsPerHost)
                                        .setSeeds(seeds)).forKeyspace(keyspace)
                        .withAstyanaxConfiguration(config)
                        .buildKeyspace(ThriftFamilyFactory.getInstance());

                return ctx;
            }

            @Override
            protected void configure() {
                bind(AggregationCacheBackend.class).to(CassandraCache.class)
                .in(Scopes.SINGLETON);
                expose(AggregationCacheBackend.class);
            }
        };
    }
}
