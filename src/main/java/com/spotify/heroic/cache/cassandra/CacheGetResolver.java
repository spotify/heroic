package com.spotify.heroic.cache.cassandra;

import java.util.ArrayList;
import java.util.List;

import lombok.RequiredArgsConstructor;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.cache.cassandra.model.CacheKey;
import com.spotify.heroic.cache.cassandra.model.CacheKeySerializer;
import com.spotify.heroic.cache.model.CacheBackendGetResult;
import com.spotify.heroic.cache.model.CacheBackendKey;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.TimeSerie;

@RequiredArgsConstructor
public final class CacheGetResolver implements
        Callback.Resolver<CacheBackendGetResult> {
    static final String CQL_QUERY = "SELECT data_offset, data_value, data_p FROM aggregations_1200 WHERE aggregation_key = ?";
    private static final CacheKeySerializer cacheKeySerializer = CacheKeySerializer
            .get();

    private final Keyspace keyspace;
    private final ColumnFamily<Integer, String> columnFamily;
    private final CacheBackendKey key;
    private final DateRange range;

    @Override
    public CacheBackendGetResult resolve() throws Exception {
        return new CacheBackendGetResult(key, doGetRow());
    }

    private List<DataPoint> doGetRow() throws ConnectionException {
        final TimeSerie timeSerie = key.getTimeSerie();
        final AggregationGroup aggregationGroup = key.getAggregationGroup();
        final long columnSize = aggregationGroup.getSampling().getSize();

        final List<Long> bases = calculateBases(columnSize);

        final List<DataPoint> datapoints = new ArrayList<DataPoint>();

        for (long base : bases) {
            final CacheKey cacheKey = new CacheKey(timeSerie, aggregationGroup,
                    base);

            final OperationResult<CqlResult<Integer, String>> op = keyspace
                    .prepareQuery(columnFamily).withCql(CQL_QUERY)
                    .asPreparedStatement()
                    .withByteBufferValue(cacheKey, cacheKeySerializer)
                    .execute();

            final CqlResult<Integer, String> result = op.getResult();
            final Rows<Integer, String> rows = result.getRows();

            for (final Row<Integer, String> row : rows) {
                final ColumnList<String> columns = row.getColumns();
                final int offset = columns.getColumnByIndex(0)
                        .getIntegerValue();
                final double value = columns.getColumnByIndex(1)
                        .getDoubleValue();
                final float p = columns.getColumnByIndex(2).getFloatValue();

                final long timestamp = getDataPointTimestamp(base, columnSize,
                        offset);

                if (timestamp < range.getStart())
                    continue;

                datapoints.add(new DataPoint(timestamp, value, p));
            }
        }

        return datapoints;
    }

    private List<Long> calculateBases(long columnWidth) {
        final List<Long> bases = new ArrayList<Long>();

        final long baseWidth = CassandraCache.WIDTH * columnWidth;
        final long first = range.getStart() - range.getStart() % baseWidth;
        final long last = range.getEnd() - range.getEnd() % baseWidth;

        for (long i = first; i <= last; i += baseWidth) {
            bases.add(i);
        }

        return bases;
    }

    private long getDataPointTimestamp(long base, long columnWidth,
            int dataOffset) {
        return base + columnWidth * dataOffset;
    }
}