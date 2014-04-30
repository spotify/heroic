package com.spotify.heroic.cache.cassandra;

import java.nio.ByteBuffer;

import com.codahale.metrics.Timer;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CallbackRunnable;
import com.spotify.heroic.cache.model.CacheBackendPutResult;
import com.spotify.heroic.model.CacheKey;
import com.spotify.heroic.model.CacheKeySerializer;
import com.spotify.heroic.model.DataPoint;

final class CachePutRunnable extends
        CallbackRunnable<CacheBackendPutResult> {
    private static final String CQL_STMT = "INSERT INTO aggregations_1200 (aggregation_key, data_offset, data_value) VALUES(?, ?, ?)";

    private final Keyspace keyspace;
    private final ColumnFamily<Integer, String> columnFamily;
    private final CacheKey key;
    private final DataPoint[] datapoints;

    CachePutRunnable(String task, Timer timer,
            Callback<CacheBackendPutResult> callback, Keyspace keyspace, ColumnFamily<Integer, String> columnFamily, CacheKey key,
            DataPoint[] datapoints) {
        super(task, timer, callback);
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
        this.key = key;
        this.datapoints = datapoints;
    }

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

    private void doPut(CacheKey key, Integer dataOffset, DataPoint dataPoint)
            throws ConnectionException {
        final ByteBuffer keyBytes = CacheKeySerializer.get().toByteBuffer(key);

        keyspace.prepareQuery(columnFamily).withCql(CQL_STMT)
                .asPreparedStatement().withValue(keyBytes)
                .withIntegerValue(dataOffset)
                .withDoubleValue(dataPoint.getValue()).execute();
    }
}