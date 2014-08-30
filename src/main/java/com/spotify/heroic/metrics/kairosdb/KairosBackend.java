package com.spotify.heroic.metrics.kairosdb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
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
import com.spotify.heroic.metrics.model.BackendEntry;
import com.spotify.heroic.metrics.model.FetchData;
import com.spotify.heroic.metrics.model.WriteMetric;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Series;
import com.spotify.heroic.model.WriteResult;
import com.spotify.heroic.statistics.BackendReporter;
import com.spotify.heroic.yaml.ConfigContext;
import com.spotify.heroic.yaml.ConfigUtils;
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
            final long start = DataPointColumnKey.toStartColumn(range.start(),
                    timestamp);
            final long end = DataPointColumnKey.toEndColumn(range.end(),
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
        private int maxConnectionsPerHost = 50;

        /**
         * Threads dedicated to asynchronous request handling.
         */
        private int readThreads = 50;

        /**
         * Threads dedicated to asynchronous request handling.
         */
        private int readQueueSize = 10000;

        @Override
        public Backend buildDelegate(String id, ConfigContext ctx,
                BackendReporter reporter) throws ValidationException {
            final String keyspace = ConfigUtils.notEmpty(
                    ctx.extend("keyspace"), this.keyspace);
            final String seeds = ConfigUtils.notEmpty(ctx.extend("seeds"),
                    this.seeds);

            final ReadWriteThreadPools pools = ReadWriteThreadPools.config()
                    .reporter(reporter.newThreadPoolsReporter())
                    .readThreads(readThreads).readQueueSize(readQueueSize)
                    .build();

            return new KairosBackend(id, pools, keyspace, seeds,
                    maxConnectionsPerHost);
        }

        @Override
        protected String defaultGroup() {
            return "kairosdb";
        }
    }

    private static final ColumnFamily<DataPointsRowKey, Integer> DATA_POINTS_CF = new ColumnFamily<DataPointsRowKey, Integer>(
            "data_points", DataPointsRowKey.Serializer.get(),
            IntegerSerializer.get());

    private final ReadWriteThreadPools pools;

    public KairosBackend(String id, ReadWriteThreadPools pools,
            String keyspace, String seeds, int maxConnectionsPerHost) {
        super(id, keyspace, seeds, maxConnectionsPerHost);
        this.pools = pools;
    }

    private static final ColumnFamily<Integer, String> CQL3_CF = ColumnFamily
            .newColumnFamily("Cql3CF", IntegerSerializer.get(),
                    StringSerializer.get());

    @Override
    public List<Callback<FetchData>> fetch(final Series series,
            final DateRange range) {
        final List<Callback<FetchData>> queries = new ArrayList<Callback<FetchData>>();

        for (final long base : buildBases(range)) {
            final Callback<FetchData> partial = buildQuery(series, base, range);

            if (partial == null)
                continue;

            queries.add(partial);
        }

        return queries;
    }

    private Callback<FetchData> buildQuery(final Series series, long base,
            DateRange queryRange) {
        final Keyspace keyspace = keyspace();

        if (keyspace == null)
            return new CancelledCallback<FetchData>(
                    CancelReason.BACKEND_DISABLED);

        final DataPointsRowKey rowKey = new DataPointsRowKey(series.getKey(),
                base, series.getTags());
        final DateRange rowRange = new DateRange(base, base
                + DataPointsRowKey.MAX_WIDTH);
        final DateRange range = queryRange.modify(rowRange);

        if (range.isEmpty())
            return null;

        final int start = DataPointColumnKey.toStartColumn(range.start(), base);
        final int end = DataPointColumnKey.toEndColumn(range.end(), base);

        final ByteBufferRange columnRange = new RangeBuilder()
        .setStart(start, IntegerSerializer.get())
        .setEnd(end, IntegerSerializer.get()).build();

        final RowQuery<DataPointsRowKey, Integer> dataQuery = keyspace
                .prepareQuery(DATA_POINTS_CF).getRow(rowKey).autoPaginate(true)
                .withColumnRange(columnRange);

        return ConcurrentCallback.newResolve(pools.read(),
                new Callback.Resolver<FetchData>() {
            @Override
            public FetchData resolve() throws Exception {
                final OperationResult<ColumnList<Integer>> result = dataQuery
                        .execute();
                final List<DataPoint> datapoints = rowKey
                        .buildDataPoints(result.getResult());
                return new FetchData(series, datapoints);
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
            final DataPointsRowKey row = new DataPointsRowKey(series.getKey(),
                    base, series.getTags());
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
    public Callback<WriteResult> write(WriteMetric write) {
        return new FailedCallback<WriteResult>(new Exception("not implemented"));
    }

    @Override
    public Callback<WriteResult> write(Collection<WriteMetric> writes) {
        return new FailedCallback<WriteResult>(new Exception("not implemented"));
    }

    @Override
    public Iterable<BackendEntry> listEntries() {
        final Keyspace keyspace = keyspace();

        if (keyspace == null)
            throw new IllegalStateException("Backend is not ready");

        final OperationResult<Rows<DataPointsRowKey, Integer>> result;

        try {
            result = keyspace.prepareQuery(DATA_POINTS_CF).getAllRows()
                    .execute();
        } catch (final ConnectionException e) {
            throw new RuntimeException("Request failed", e);
        }

        return new Iterable<BackendEntry>() {
            @Override
            public Iterator<BackendEntry> iterator() {
                final Iterator<Row<DataPointsRowKey, Integer>> iterator = result
                        .getResult().iterator();

                return new Iterator<BackendEntry>() {
                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    public BackendEntry next() {
                        final Row<DataPointsRowKey, Integer> entry = iterator
                                .next();
                        final DataPointsRowKey rowKey = entry.getKey();
                        final Series series = new Series(
                                rowKey.getMetricName(), rowKey.getTags());

                        final List<DataPoint> dataPoints = rowKey
                                .buildDataPoints(entry.getColumns());

                        return new BackendEntry(series, dataPoints);
                    }
                };
            }
        };
    }
}
