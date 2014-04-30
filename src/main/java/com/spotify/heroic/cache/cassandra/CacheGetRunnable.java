package com.spotify.heroic.cache.cassandra;

import java.nio.ByteBuffer;

import com.codahale.metrics.Timer;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CallbackRunnable;
import com.spotify.heroic.cache.AggregationCache;
import com.spotify.heroic.cache.model.CacheBackendGetResult;
import com.spotify.heroic.model.CacheKey;
import com.spotify.heroic.model.CacheKeySerializer;
import com.spotify.heroic.model.DataPoint;

public final class CacheGetRunnable extends
        CallbackRunnable<CacheBackendGetResult> {
    static final String CQL_QUERY = "SELECT data_offset, data_value FROM aggregation_1200 WHERE aggregation_key = ?";

    private final Keyspace keyspace;
    private final ColumnFamily<Integer, String> columnFamily;
    private final CacheKey key;

    public CacheGetRunnable(String task, Timer timer,
            Callback<CacheBackendGetResult> callback, Keyspace keyspace, ColumnFamily<Integer, String> columnFamily, CacheKey key) {
        super(task, timer, callback);
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
        this.key = key;
    }

    @Override
    public CacheBackendGetResult execute() throws Exception {
        return new CacheBackendGetResult(key, doGetRow(key));
    }

    private DataPoint[] doGetRow(CacheKey key) throws ConnectionException {
        final DataPoint[] dataPoints = new DataPoint[AggregationCache.WIDTH];

        final long columnWidth = key.getAggregation().getWidth();
        final long base = key.getBase();

        final ByteBuffer keyBytes = CacheKeySerializer.get().toByteBuffer(key);
        final OperationResult<CqlResult<Integer, String>> op = keyspace
                .prepareQuery(columnFamily).withCql(CQL_QUERY)
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
}