package com.spotify.heroic.aggregationcache.cassandra2;

import java.util.List;

import lombok.RequiredArgsConstructor;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.aggregationcache.model.CacheBackendKey;
import com.spotify.heroic.aggregationcache.model.CacheBackendPutResult;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.model.CacheKey;
import com.spotify.heroic.model.CacheKeySerializer;
import com.spotify.heroic.model.DataPoint;

@RequiredArgsConstructor
final class CachePutResolver implements Callback.Resolver<CacheBackendPutResult> {
    private static final String CQL_STMT = "INSERT INTO aggregations_1200 (aggregation_key, data_offset, data_value) VALUES(?, ?, ?)";

    private final CacheKeySerializer cacheKeySerializer;
    private final Keyspace keyspace;
    private final ColumnFamily<Integer, String> columnFamily;
    private final CacheBackendKey key;
    private final List<DataPoint> datapoints;

    @Override
    public CacheBackendPutResult resolve() throws Exception {
        final AggregationGroup aggregation = key.getAggregation();
        final long size = aggregation.getSampling().getSize();
        final long columnWidth = size * Cassandra2AggregationCacheBackend.WIDTH;
        for (final DataPoint d : datapoints) {
            final double value = d.getValue();

            if (Double.isNaN(value))
                continue;

            final int index = (int) ((d.getTimestamp() % columnWidth) / size);
            final long base = d.getTimestamp() - d.getTimestamp() % columnWidth;
            final CacheKey key = new CacheKey(CacheKey.VERSION, this.key.getFilter(), this.key.getGroup(), aggregation,
                    base);
            doPut(key, index, d);
        }

        return new CacheBackendPutResult();
    }

    private void doPut(CacheKey key, Integer dataOffset, DataPoint d) throws ConnectionException {
        keyspace.prepareQuery(columnFamily).withCql(CQL_STMT).asPreparedStatement()
                .withByteBufferValue(key, cacheKeySerializer).withIntegerValue(dataOffset)
                .withDoubleValue(d.getValue()).execute();
    }
}