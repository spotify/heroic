package com.spotify.heroic.cache.cassandra;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import javax.inject.Inject;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.netflix.astyanax.AstyanaxConfiguration;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.cache.AggregationCacheBackend;
import com.spotify.heroic.cache.CacheOperationException;
import com.spotify.heroic.cache.model.CacheBackendGetResult;
import com.spotify.heroic.cache.model.CacheBackendKey;
import com.spotify.heroic.cache.model.CacheBackendPutResult;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.statistics.AggregationCacheBackendReporter;

public class CassandraCache implements AggregationCacheBackend {
    public static final int WIDTH = 1200;

    public static final int DEFAULT_THREADS = 20;
    public static final String DEFAULT_KEYSPACE = "aggregations";
    public static final int DEFAULT_MAX_CONNECTIONS_PER_HOST = 20;

    @JsonCreator
    public static CassandraCache create(
            @JsonProperty("seeds") String seeds,
            @JsonProperty("keyspace") String keyspace,
            @JsonProperty("maxConnectionsPerHost") Integer maxConnectionsPerHost,
            @JsonProperty("threads") Integer threads) {
        if (seeds == null)
            throw new RuntimeException("'seeds' must be defined");

        if (keyspace == null)
            keyspace = DEFAULT_KEYSPACE;

        if (maxConnectionsPerHost == null)
            maxConnectionsPerHost = DEFAULT_MAX_CONNECTIONS_PER_HOST;

        if (threads == null)
            threads = DEFAULT_THREADS;

        final Executor executor = Executors.newFixedThreadPool(threads);

        return new CassandraCache(executor, seeds, keyspace,
                maxConnectionsPerHost);
    }

    private final Keyspace keyspace;
    private final Executor executor;

    private final ColumnFamily<Integer, String> CQL3_CF = ColumnFamily
            .newColumnFamily("Cql3CF", IntegerSerializer.get(),
                    StringSerializer.get());

    @Inject
    private AggregationCacheBackendReporter reporter;

    public CassandraCache(Executor executor, String seeds, String keyspace,
            int maxConnectionsPerHost) {
        this.executor = executor;

        final AstyanaxConfiguration config = new AstyanaxConfigurationImpl()
                .setCqlVersion("3.0.0").setTargetCassandraVersion("2.0");
        final AstyanaxContext<Keyspace> ctx = new AstyanaxContext.Builder()
                .withConnectionPoolConfiguration(
                        new ConnectionPoolConfigurationImpl(
                                "HeroicConnectionPool").setPort(9160)
                                .setMaxConnsPerHost(maxConnectionsPerHost)
                                .setSeeds(seeds)).forKeyspace(keyspace)
                .withAstyanaxConfiguration(config)
                .buildKeyspace(ThriftFamilyFactory.getInstance());

        ctx.start();
        this.keyspace = ctx.getClient();
    }

    @Override
    public Callback<CacheBackendGetResult> get(final CacheBackendKey key,
            DateRange range) throws CacheOperationException {
        return ConcurrentCallback.newResolve(executor, new CacheGetResolver(
                keyspace, CQL3_CF, key, range));
    }

    @Override
    public Callback<CacheBackendPutResult> put(final CacheBackendKey key,
            final List<DataPoint> datapoints) throws CacheOperationException {
        return ConcurrentCallback.newResolve(executor, new CachePutResolver(
                keyspace, CQL3_CF, key, datapoints));
    }
}
