package com.spotify.heroic.cache.cassandra;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import lombok.Getter;
import lombok.Setter;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
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
import com.spotify.heroic.yaml.Utils;
import com.spotify.heroic.yaml.ValidationException;

public class CassandraCache implements
        AggregationCacheBackend {
    public static final int WIDTH = 1200;

    public static class YAML implements AggregationCacheBackend.YAML {
        public static final String TYPE = "!cassandra-cache";

        /**
         * Cassandra seed nodes.
         */
        @Getter
        @Setter
        private String seeds;

        /**
         * Cassandra keyspace for aggregations data.
         */
        @Getter
        @Setter
        private String keyspace = "aggregations";

        /**
         * Max connections per host in the cassandra cluster.
         */
        @Getter
        @Setter
        private int maxConnectionsPerHost = 20;

        /**
         * Threads dedicated to asynchronous request handling.
         */
        @Getter
        @Setter
        private int threads = 20;

        @Override
        public AggregationCacheBackend build(String context,
                MetricRegistry registry) throws ValidationException {
            Utils.notEmpty(context + ".keyspace", this.keyspace);
            Utils.notEmpty(context + ".seeds", this.seeds);
            final Executor executor = Executors.newFixedThreadPool(threads);
            return new CassandraCache(registry, executor,
                    seeds, keyspace, maxConnectionsPerHost);
        }
    }

    private final Keyspace keyspace;
    private final Executor executor;

    private final ColumnFamily<Integer, String> CQL3_CF = ColumnFamily
            .newColumnFamily("Cql3CF", IntegerSerializer.get(),
                    StringSerializer.get());

    private final Map<String, Timer> timerPool;
    private final MetricRegistry metricsRegistry;

    public CassandraCache(MetricRegistry registry,
            Executor executor, String seeds, String keyspace,
            int maxConnectionsPerHost) {
        this.metricsRegistry = registry;
        this.executor = executor;
        timerPool = new HashMap<String, Timer>();

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
    public Callback<CacheBackendGetResult> get(final CacheBackendKey key, DateRange range)
            throws AggregationCacheException {
        final ConcurrentCallback<CacheBackendGetResult> callback = new ConcurrentCallback<CacheBackendGetResult>();
        final String taskName = "get-cache-row";
        final Timer timer = getTimer(taskName);

        executor.execute(new CacheGetRunnable(taskName, timer, callback, keyspace, CQL3_CF, key, range));

        return callback;
    }

    private Timer getTimer(String taskName) {
        Timer timer = timerPool.get(taskName);

        if (timer == null) {
            timer = metricsRegistry.timer(MetricRegistry.name("heroic",
                    "cache", "aggregation", "cassandra", taskName));
            timerPool.put(taskName, timer);
        }

        return timer;
    }

    @Override
    public Callback<CacheBackendPutResult> put(final CacheBackendKey key,
            final List<DataPoint> datapoints) throws AggregationCacheException {
        final ConcurrentCallback<CacheBackendPutResult> callback = new ConcurrentCallback<CacheBackendPutResult>();
        final String taskName = "put-cache-row";
        final Timer timer = getTimer(taskName);


        executor.execute(new CachePutRunnable(taskName, timer, callback, keyspace, CQL3_CF, key, datapoints));
        return callback;
    }
}
