package com.spotify.heroic.aggregationcache.cassandra2;

import java.util.List;
import java.util.concurrent.Callable;

import lombok.RequiredArgsConstructor;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregationcache.model.CacheBackendKey;
import com.spotify.heroic.aggregationcache.model.CacheBackendPutResult;
import com.spotify.heroic.model.CacheKey;
import com.spotify.heroic.model.DataPoint;

@RequiredArgsConstructor
final class CachePutResolver implements Callable<CacheBackendPutResult> {
    private static final String CQL_STMT = "INSERT INTO aggregations_1200 (aggregation_key, data_offset, data_value) VALUES(?, ?, ?)";

    private final Serializer<CacheKey> cacheKeySerializer;
    private final Context ctx;
    private final ColumnFamily<Integer, String> columnFamily;
    private final CacheBackendKey key;
    private final List<DataPoint> datapoints;

    @Override
    public CacheBackendPutResult call() throws Exception {
        final Keyspace keyspace = ctx.getClient();
        final Aggregation aggregation = key.getAggregation();
        final long size = aggregation.sampling().getSize();
        final long columnWidth = size * Cassandra2AggregationCacheBackend.WIDTH;

        for (final DataPoint d : datapoints) {
            final double value = d.getValue();

            if (Double.isNaN(value))
                continue;

            final int index = (int) ((d.getTimestamp() % columnWidth) / size);
            final long base = d.getTimestamp() - d.getTimestamp() % columnWidth;
            final CacheKey key = new CacheKey(CacheKey.VERSION, this.key.getFilter(), this.key.getGroup(), aggregation,
                    base);
            doPut(keyspace, key, index, d);
        }

        return new CacheBackendPutResult();
    }

    private void doPut(Keyspace keyspace, CacheKey key, Integer dataOffset, DataPoint d) throws ConnectionException {
        keyspace.prepareQuery(columnFamily).withCql(CQL_STMT).asPreparedStatement()
                .withByteBufferValue(key, cacheKeySerializer).withIntegerValue(dataOffset)
                .withDoubleValue(d.getValue()).execute();
    }
}