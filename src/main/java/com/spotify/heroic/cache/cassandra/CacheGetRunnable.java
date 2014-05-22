package com.spotify.heroic.cache.cassandra;

import java.util.ArrayList;
import java.util.List;

import com.codahale.metrics.Timer;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.spotify.heroic.aggregator.AggregationGroup;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CallbackRunnable;
import com.spotify.heroic.cache.model.CacheBackendGetResult;
import com.spotify.heroic.cache.model.CacheBackendKey;
import com.spotify.heroic.model.CacheKey;
import com.spotify.heroic.model.CacheKeySerializer;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.TimeSerie;

public final class CacheGetRunnable extends
        CallbackRunnable<CacheBackendGetResult> {
    static final String CQL_QUERY = "SELECT data_offset, data_value FROM aggregations_1200 WHERE aggregation_key = ?";
    private static final CacheKeySerializer cacheKeySerializer = CacheKeySerializer.get();

    private final Keyspace keyspace;
    private final ColumnFamily<Integer, String> columnFamily;
    private final CacheBackendKey key;
    private final DateRange range;

    public CacheGetRunnable(String task, Timer timer,
            Callback<CacheBackendGetResult> callback, Keyspace keyspace, ColumnFamily<Integer, String> columnFamily, CacheBackendKey key, DateRange range) {
        super(task, timer, callback);
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
        this.key = key;
        this.range = range;
    }

    @Override
    public CacheBackendGetResult execute() throws Exception {
        return new CacheBackendGetResult(key, doGetRow());
    }

    private List<DataPoint> doGetRow() throws ConnectionException {
        final TimeSerie timeSerie = key.getTimeSerie();
        final AggregationGroup aggregationGroup = key.getAggregationGroup();
        final long columnWidth = aggregationGroup.getWidth();

        final List<Long> bases = calculateBases(columnWidth);

        final List<DataPoint> datapoints = new ArrayList<DataPoint>();

        for (long base : bases) {
            final CacheKey cacheKey = new CacheKey(timeSerie, aggregationGroup, base);

            final OperationResult<CqlResult<Integer, String>> op = keyspace
                    .prepareQuery(columnFamily).withCql(CQL_QUERY)
                    .asPreparedStatement()
                    .withByteBufferValue(cacheKey, cacheKeySerializer).execute();

            final CqlResult<Integer, String> result = op.getResult();
            final Rows<Integer, String> rows = result.getRows();

            for (final Row<Integer, String> row : rows) {
                final ColumnList<String> columns = row.getColumns();
                final int offset = columns.getColumnByIndex(0).getIntegerValue();
                final double value = columns.getColumnByIndex(1).getDoubleValue();

                final long timestamp = getDataPointTimestamp(base,
                        columnWidth, offset);

                if (timestamp < range.getStart())
                    continue;

                datapoints.add(new DataPoint(timestamp, value));
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