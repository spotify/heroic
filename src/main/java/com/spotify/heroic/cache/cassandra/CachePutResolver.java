package com.spotify.heroic.cache.cassandra;

import java.util.List;

import lombok.RequiredArgsConstructor;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.cache.cassandra.model.CacheKey;
import com.spotify.heroic.cache.cassandra.model.CacheKeySerializer;
import com.spotify.heroic.cache.model.CacheBackendKey;
import com.spotify.heroic.cache.model.CacheBackendPutResult;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.TimeSerie;

@RequiredArgsConstructor
final class CachePutResolver implements
Callback.Resolver<CacheBackendPutResult> {
    private static final String CQL_STMT = "INSERT INTO aggregations_1200 (aggregation_key, data_offset, data_value) VALUES(?, ?, ?)";
    private static final CacheKeySerializer cacheKeySerializer = CacheKeySerializer.get();

    private final Keyspace keyspace;
    private final ColumnFamily<Integer, String> columnFamily;
    private final CacheBackendKey key;
    private final List<DataPoint> datapoints;

    @Override
    public CacheBackendPutResult resolve() throws Exception {
        final AggregationGroup aggregation = key.getAggregationGroup();
        final TimeSerie timeSerie = key.getTimeSerie();
        final long size = aggregation.getSampling().getSize();
        final long columnWidth = size * CassandraCache.WIDTH;

        for (final DataPoint d : datapoints) {
            final double value = d.getValue();
            final float p = d.getP();

            if (Double.isNaN(value))
                continue;

            /* datapoint has no backing in reality, don't cache. */
            if (Float.isNaN(p))
                continue;

            int index = (int)((d.getTimestamp() % columnWidth) / size);
            long base = d.getTimestamp() - d.getTimestamp() % columnWidth;
            final CacheKey key = new CacheKey(timeSerie, aggregation, base);
            doPut(key, index, d);
        }

        return new CacheBackendPutResult();
    }

    private void doPut(CacheKey key, Integer dataOffset, DataPoint dataPoint)
            throws ConnectionException {
        keyspace.prepareQuery(columnFamily).withCql(CQL_STMT)
        .asPreparedStatement().withByteBufferValue(key, cacheKeySerializer)
        .withIntegerValue(dataOffset)
        .withDoubleValue(dataPoint.getValue()).execute();
    }
}