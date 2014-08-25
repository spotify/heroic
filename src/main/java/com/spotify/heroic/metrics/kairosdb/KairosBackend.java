package com.spotify.heroic.metrics.kairosdb;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.util.RangeBuilder;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.CancelledCallback;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.async.FailedCallback;
import com.spotify.heroic.concurrrency.ReadWriteThreadPools;
import com.spotify.heroic.metrics.Backend;
import com.spotify.heroic.metrics.cassandra.CassandraBackend;
import com.spotify.heroic.metrics.model.FetchDataPoints;
import com.spotify.heroic.metrics.model.FetchDataPoints.Result;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Series;
import com.spotify.heroic.model.WriteMetric;
import com.spotify.heroic.model.WriteResponse;
import com.spotify.heroic.statistics.MetricBackendReporter;
import com.spotify.heroic.yaml.Utils;
import com.spotify.heroic.yaml.ValidationException;

/**
 * The data access layer for accessing KairosDB schema in Cassandra.
 *
 * @author mehrdad
 */
@ToString(of = {}, callSuper = true)
public class KairosBackend extends CassandraBackend implements Backend {
    private static final String COUNT_CQL = "SELECT count(*) FROM data_points WHERE key = ? AND "
            + "column1 > ? AND column1 < ?";

    @RequiredArgsConstructor
    private static final class RowCountTransformer implements
            Callback.Resolver<Long> {
        private final Keyspace keyspace;
        private final DateRange range;
        private final DataPointsRowKey row;

        @Override
        public Long resolve() throws Exception {
            final long timestamp = row.getTimestamp();
            final long start = DataPointColumnKey.toStartTimeStamp(
                    range.start(), timestamp);
            final long end = DataPointColumnKey.toEndTimeStamp(range.end(),
                    timestamp);

            final OperationResult<CqlResult<Integer, String>> op = keyspace
                    .prepareQuery(CQL3_CF)
                    .withCql(COUNT_CQL)
                    .asPreparedStatement()
                    .withByteBufferValue(row, DataPointsRowKey.Serializer.get())
                    .withByteBufferValue((int) start, IntegerSerializer.get())
                    .withByteBufferValue((int) end, IntegerSerializer.get())
                    .execute();

            final CqlResult<Integer, String> result = op.getResult();
            return result.getRows().getRowByIndex(0).getColumns()
                    .getColumnByName("count").getLongValue();
        }
    }

    @Data
    @EqualsAndHashCode(callSuper = true)
    public static class YAML extends Backend.YAML {
        public static final String TYPE = "!kairosdb-backend";

        /**
         * Cassandra seed nodes.
         */
        private String seeds;

        /**
         * Cassandra keyspace for kairosdb.
         */
        private String keyspace = "kairosdb";

        /**
         * Max connections per host in the cassandra cluster.
         */
        private int maxConnectionsPerHost = 20;

        /**
         * Threads dedicated to asynchronous request handling.
         */
        private int readThreads = 20;

        /**
         * Threads dedicated to asynchronous request handling.
         */
        private int readQueueSize = 10000;

        @Override
        public Backend buildDelegate(String context,
                MetricBackendReporter reporter) throws ValidationException {
            Utils.notEmpty(context + ".keyspace", this.keyspace);
            Utils.notEmpty(context + ".seeds", this.seeds);
            final ReadWriteThreadPools pools = ReadWriteThreadPools.config()
                    .reporter(reporter.newThreadPoolsReporter())
                    .readThreads(readThreads).readQueueSize(readQueueSize)
                    .build();
            return new KairosBackend(pools, keyspace, seeds,
                    maxConnectionsPerHost);
        }
    }

    private static final ColumnFamily<DataPointsRowKey, Integer> DATA_POINTS_CF = new ColumnFamily<DataPointsRowKey, Integer>(
            "data_points", DataPointsRowKey.Serializer.get(),
            IntegerSerializer.get());

    private final ReadWriteThreadPools pools;

    public KairosBackend(ReadWriteThreadPools pools, String keyspace,
            String seeds, int maxConnectionsPerHost) {
        super(keyspace, seeds, maxConnectionsPerHost);
        this.pools = pools;
    }

    private static final ColumnFamily<Integer, String> CQL3_CF = ColumnFamily
            .newColumnFamily("Cql3CF", IntegerSerializer.get(),
                    StringSerializer.get());

    @Override
    public List<Callback<FetchDataPoints.Result>> query(
            final Series series, final DateRange range) {
        final List<Callback<FetchDataPoints.Result>> queries = new ArrayList<Callback<FetchDataPoints.Result>>();

        for (final long base : buildBases(range)) {
            final Callback<Result> partial = buildQuery(series, base, range);

            if (partial == null)
                continue;

            queries.add(partial);
        }

        return queries;
    }

    private Callback<FetchDataPoints.Result> buildQuery(
            final Series series, long base, DateRange queryRange) {
        final Keyspace keyspace = keyspace();

        if (keyspace == null)
            return new CancelledCallback<FetchDataPoints.Result>(
                    CancelReason.BACKEND_DISABLED);

        final DataPointsRowKey rowKey = new DataPointsRowKey(
                series.getKey(), base, series.getTags());
        final DateRange rowRange = new DateRange(base, base
                + DataPointsRowKey.MAX_WIDTH);
        final DateRange range = queryRange.modify(rowRange);

        if (range.isEmpty())
            return null;

        final long startTime = DataPointColumnKey.toStartTimeStamp(
                range.start(), base);
        final long endTime = DataPointColumnKey.toEndTimeStamp(range.end(),
                base);

        final RowQuery<DataPointsRowKey, Integer> dataQuery = keyspace
                .prepareQuery(DATA_POINTS_CF)
                .getRow(rowKey)
                .autoPaginate(true)
                .withColumnRange(
                        new RangeBuilder()
                                .setStart((int) startTime,
                                        IntegerSerializer.get())
                                .setEnd((int) endTime, IntegerSerializer.get())
                                .build());

        return ConcurrentCallback.newResolve(pools.read(),
                new Callback.Resolver<FetchDataPoints.Result>() {
                    @Override
                    public Result resolve() throws Exception {
                        final OperationResult<ColumnList<Integer>> result = dataQuery
                                .execute();
                        final List<DataPoint> datapoints = buildDataPoints(
                                rowKey, result);
                        return new FetchDataPoints.Result(datapoints, series);
                    }

                    private List<DataPoint> buildDataPoints(
                            final DataPointsRowKey rowKey,
                            final OperationResult<ColumnList<Integer>> result) {
                        final List<DataPoint> datapoints = new ArrayList<DataPoint>();

                        for (final Column<Integer> column : result.getResult()) {
                            datapoints.add(fromColumn(rowKey, column));
                        }

                        return datapoints;
                    }

                    private DataPoint fromColumn(DataPointsRowKey rowKey,
                            Column<Integer> column) {
                        final int name = column.getName();
                        final long timestamp = DataPointColumnKey.toTimeStamp(
                                rowKey.getTimestamp(), name);
                        final ByteBuffer bytes = column.getByteBufferValue();

                        if (DataPointColumnKey.isLong(name))
                            return new DataPoint(timestamp,
                                    DataPointColumnValue.toLong(bytes));

                        return new DataPoint(timestamp, DataPointColumnValue
                                .toDouble(bytes));
                    }
                });
    }

    @Override
    public Callback<Long> getColumnCount(final Series series,
            final DateRange range) {
        final Keyspace keyspace = keyspace();

        if (keyspace == null)
            return new CancelledCallback<Long>(CancelReason.BACKEND_DISABLED);

        final List<Callback<Long>> callbacks = new ArrayList<Callback<Long>>();

        for (final long base : buildBases(range)) {
            final DataPointsRowKey row = new DataPointsRowKey(
                    series.getKey(), base, series.getTags());
            callbacks.add(ConcurrentCallback.newResolve(pools.read(),
                    new RowCountTransformer(keyspace, range, row)));
        }

        return ConcurrentCallback.newReduce(callbacks,
                new Callback.Reducer<Long, Long>() {
                    @Override
                    public Long resolved(Collection<Long> results,
                            Collection<Exception> errors,
                            Collection<CancelReason> cancelled)
                            throws Exception {
                        long value = 0;

                        for (final long result : results) {
                            value += result;
                        }

                        return value;
                    }
                });
    }

    private static List<Long> buildBases(DateRange range) {
        final List<Long> bases = new ArrayList<Long>();

        final long start = range.getStart() - range.getStart()
                % DataPointsRowKey.MAX_WIDTH;
        final long end = range.getEnd() - range.getEnd()
                % DataPointsRowKey.MAX_WIDTH + DataPointsRowKey.MAX_WIDTH;

        for (long i = start; i < end; i += DataPointsRowKey.MAX_WIDTH) {
            bases.add(i);
        }

        return bases;
    }

    @Override
    public Callback<WriteResponse> write(WriteMetric write) {
        return new FailedCallback<WriteResponse>(new Exception(
                "not implemented"));
    }

    @Override
    public Callback<WriteResponse> write(Collection<WriteMetric> writes) {
        return new FailedCallback<WriteResponse>(new Exception(
                "not implemented"));
    }
}
