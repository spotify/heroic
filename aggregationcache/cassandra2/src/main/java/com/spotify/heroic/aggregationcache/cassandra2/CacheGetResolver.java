package com.spotify.heroic.aggregationcache.cassandra2;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import lombok.RequiredArgsConstructor;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregationcache.model.CacheBackendGetResult;
import com.spotify.heroic.aggregationcache.model.CacheBackendKey;
import com.spotify.heroic.model.CacheKey;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;

@RequiredArgsConstructor
public final class CacheGetResolver implements Callable<CacheBackendGetResult> {
    static final String CQL_QUERY = "SELECT data_offset, data_value FROM aggregations_1200 WHERE aggregation_key = ?";

    private final Serializer<CacheKey> cacheKeySerializer;
    private final Context ctx;
    private final ColumnFamily<Integer, String> columnFamily;
    private final CacheBackendKey key;
    private final DateRange range;

    @Override
    public CacheBackendGetResult call() throws Exception {
        return new CacheBackendGetResult(key, doGetRow());
    }

    private List<DataPoint> doGetRow() throws ConnectionException {
        final Keyspace keyspace = ctx.getClient();
        final Aggregation aggregation = key.getAggregation();
        final long columnSize = aggregation.sampling().getSize();

        final List<Long> bases = calculateBases(columnSize);

        final List<DataPoint> datapoints = new ArrayList<DataPoint>();

        for (final long base : bases) {
            final CacheKey cacheKey = new CacheKey(CacheKey.VERSION, key.getFilter(), key.getGroup(), aggregation, base);

            final OperationResult<CqlResult<Integer, String>> op = keyspace.prepareQuery(columnFamily)
                    .withCql(CQL_QUERY).asPreparedStatement().withByteBufferValue(cacheKey, cacheKeySerializer)
                    .execute();

            final CqlResult<Integer, String> result = op.getResult();
            final Rows<Integer, String> rows = result.getRows();

            for (final Row<Integer, String> row : rows) {
                final ColumnList<String> columns = row.getColumns();
                final int offset = columns.getColumnByIndex(0).getIntegerValue();
                final double value = columns.getColumnByIndex(1).getDoubleValue();

                final long timestamp = getDataPointTimestamp(base, columnSize, offset);

                if (timestamp < range.getStart() || timestamp > range.getEnd())
                    continue;

                datapoints.add(new DataPoint(timestamp, value));
            }
        }

        return datapoints;
    }

    private List<Long> calculateBases(long columnWidth) {
        final List<Long> bases = new ArrayList<Long>();

        final long baseWidth = Cassandra2AggregationCacheBackend.WIDTH * columnWidth;
        final long first = range.getStart() - range.getStart() % baseWidth;
        final long last = range.getEnd() - range.getEnd() % baseWidth;

        for (long i = first; i <= last; i += baseWidth) {
            bases.add(i);
        }

        return bases;
    }

    private long getDataPointTimestamp(long base, long columnWidth, int dataOffset) {
        return base + columnWidth * dataOffset;
    }
}