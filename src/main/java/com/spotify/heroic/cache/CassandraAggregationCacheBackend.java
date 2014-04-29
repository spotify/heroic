package com.spotify.heroic.cache;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.netflix.astyanax.AstyanaxConfiguration;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CallbackRunnable;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.cache.model.CacheBackendGetResult;
import com.spotify.heroic.cache.model.CacheBackendPutResult;
import com.spotify.heroic.model.CacheKey;
import com.spotify.heroic.model.CacheKeySerializer;
import com.spotify.heroic.model.DataPoint;

public class CassandraAggregationCacheBackend implements
        AggregationCacheBackend {

    private final Keyspace keyspace;
    private final Executor executor;

    private static final String GET_ENTRY_QUERY = "SELECT data_offset, data_value FROM aggregation_1200 WHERE aggregation_key = ?";
    private static final String PUT_ENTRY_QUERY = "INSERT INTO aggregations_1200 (aggregation_key, data_offset, data_value) VALUES(?, ?, ?)";

    private final ColumnFamily<Integer, String> CQL3_CF = ColumnFamily
            .newColumnFamily("Cql3CF", IntegerSerializer.get(),
                    StringSerializer.get());

    private final Map<String, Timer> timerPool;
    private final MetricRegistry metricsRegistry;

    public CassandraAggregationCacheBackend(MetricRegistry registry,
            String seeds, String keyspace, int maxConnectionsPerHost) {
        this.metricsRegistry = registry;
        executor = Executors.newFixedThreadPool(20);
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

    private DataPoint[] doGetRow(CacheKey key) throws ConnectionException {
        final DataPoint[] dataPoints = new DataPoint[AggregationCache.WIDTH];

        final long columnWidth = key.getAggregation().getSampling().getWidth();
        final long base = key.getBase();

        final ByteBuffer keyBytes = CacheKeySerializer.get().toByteBuffer(key);
        final OperationResult<CqlResult<Integer, String>> op = keyspace
                .prepareQuery(CQL3_CF).withCql(GET_ENTRY_QUERY)
                .asPreparedStatement().withValue(keyBytes).execute();
        final CqlResult<Integer, String> result = op.getResult();
        final Rows<Integer, String> rows = result.getRows();
        for (final Row<Integer, String> row : rows) {
            final ColumnList<String> columns = row.getColumns();
            final int dataOffset = columns.getColumnByName("data_offset")
                    .getIntegerValue();
            final double dataValue = columns.getColumnByName("data_value")
                    .getDoubleValue();
            dataPoints[dataOffset] = new DataPoint(getDataPointTimestamp(base,
                    columnWidth, dataOffset), dataValue);
        }
        return dataPoints;
    }

    private long getDataPointTimestamp(long base, long columnWidth,
            int dataOffset) {
        return base + columnWidth * dataOffset;
    }

    @Override
    public Callback<CacheBackendGetResult> get(final CacheKey key)
            throws AggregationCacheException {
        final ConcurrentCallback<CacheBackendGetResult> callback = new ConcurrentCallback<CacheBackendGetResult>();
        final String taskName = "get-cache-row";
        final Timer timer = getTimer(taskName);

        executor.execute(new CallbackRunnable<CacheBackendGetResult>(taskName,
                timer, callback) {

            @Override
            public CacheBackendGetResult execute() throws Exception {
                return new CacheBackendGetResult(key, doGetRow(key));
            }
        });

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
    public Callback<CacheBackendPutResult> put(final CacheKey key,
            final DataPoint[] datapoints) throws AggregationCacheException {
        final ConcurrentCallback<CacheBackendPutResult> callback = new ConcurrentCallback<CacheBackendPutResult>();
        final String taskName = "put-cache-row";
        final Timer timer = getTimer(taskName);

        executor.execute(new CallbackRunnable<CacheBackendPutResult>(taskName,
                timer, callback) {

            @Override
            public CacheBackendPutResult execute() throws Exception {
                for (int i = 0; i < datapoints.length; i++) {
                    if (datapoints[i] == null) {
                        continue;
                    }
                    doPut(key, i, datapoints[i]);
                }
                return new CacheBackendPutResult();
            }
        });
        return callback;
    }

    private void doPut(CacheKey key, Integer dataOffset, DataPoint dataPoint)
            throws ConnectionException {
        final ByteBuffer keyBytes = CacheKeySerializer.get().toByteBuffer(key);

        keyspace.prepareQuery(CQL3_CF).withCql(PUT_ENTRY_QUERY)
                .asPreparedStatement().withValue(keyBytes)
                .withIntegerValue(dataOffset)
                .withDoubleValue(dataPoint.getValue()).execute();
    }
}
