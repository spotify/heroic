package com.spotify.heroic.cache.cassandra;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import lombok.Data;

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
import com.spotify.heroic.cache.AggregationCacheException;
import com.spotify.heroic.cache.model.CacheBackendGetResult;
import com.spotify.heroic.cache.model.CacheBackendKey;
import com.spotify.heroic.cache.model.CacheBackendPutResult;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.statistics.AggregationCacheBackendReporter;
import com.spotify.heroic.yaml.ConfigContext;
import com.spotify.heroic.yaml.ConfigUtils;
import com.spotify.heroic.yaml.ValidationException;

public class CassandraCache implements AggregationCacheBackend {
    public static final int WIDTH = 1200;

    @Data
    public static class YAML implements AggregationCacheBackend.YAML {
        public static final String TYPE = "!cassandra-cache";

        /**
         * Cassandra seed nodes.
         */
        private String seeds;

        /**
         * Cassandra keyspace for aggregations data.
         */
        private String keyspace = "aggregations";

        /**
         * Max connections per host in the cassandra cluster.
         */
        private int maxConnectionsPerHost = 20;

        /**
         * Threads dedicated to asynchronous request handling.
         */
        private int threads = 20;

        @Override
        public AggregationCacheBackend build(ConfigContext ctx,
                AggregationCacheBackendReporter reporter)
                throws ValidationException {
            ConfigUtils.notEmpty(ctx.extend("keyspace"), this.keyspace);
            ConfigUtils.notEmpty(ctx.extend("seeds"), this.seeds);
            final Executor executor = Executors.newFixedThreadPool(threads);
            return new CassandraCache(reporter, executor, seeds, keyspace,
                    maxConnectionsPerHost);
        }
    }

    private final Keyspace keyspace;
    private final Executor executor;

    private final ColumnFamily<Integer, String> CQL3_CF = ColumnFamily
            .newColumnFamily("Cql3CF", IntegerSerializer.get(),
                    StringSerializer.get());

    @SuppressWarnings("unused")
    private final AggregationCacheBackendReporter reporter;

    public CassandraCache(AggregationCacheBackendReporter reporter,
            Executor executor, String seeds, String keyspace,
            int maxConnectionsPerHost) {
        this.reporter = reporter;
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
            DateRange range) throws AggregationCacheException {
        return ConcurrentCallback.newResolve(executor, new CacheGetResolver(
                keyspace, CQL3_CF, key, range));
    }

    @Override
    public Callback<CacheBackendPutResult> put(final CacheBackendKey key,
            final List<DataPoint> datapoints) throws AggregationCacheException {
        return ConcurrentCallback.newResolve(executor, new CachePutResolver(
                keyspace, CQL3_CF, key, datapoints));
    }
}
