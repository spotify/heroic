package com.spotify.heroic.metrics.heroic;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.serializers.DoubleSerializer;
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
import com.spotify.heroic.metrics.model.WriteBatchResult;
import com.spotify.heroic.metrics.model.WriteMetric;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Series;
import com.spotify.heroic.statistics.BackendReporter;
import com.spotify.heroic.yaml.ConfigContext;
import com.spotify.heroic.yaml.ConfigUtils;
import com.spotify.heroic.yaml.ValidationException;

/**
 * MetricBackend for Heroic cassandra datastore.
 */
@ToString(of = {}, callSuper = true)
@Slf4j
public class HeroicBackend extends CassandraBackend implements Backend {
    @Data
    @EqualsAndHashCode(callSuper = true)
    public static class YAML extends Backend.YAML {
        public static final String TYPE = "!heroic-backend";

        /**
         * Cassandra seed nodes.
         */
        private String seeds;

        /**
         * Cassandra keyspace for heroic.
         */
        private String keyspace = "heroic";

        /**
         * Max connections per host in the cassandra cluster.
         */
        private int maxConnectionsPerHost = 50;

        /**
         * Threads dedicated to asynchronous request handling.
         */
        private int readThreads = 50;
        private int readQueueSize = 10000;

        /**
         * Threads dedicated to asynchronous request handling.
         */
        private int writeThreads = 50;
        private int writeQueueSize = 1000;

        @Override
        public Backend buildDelegate(String id, ConfigContext ctx,
                BackendReporter reporter) throws ValidationException {
            ConfigUtils.notEmpty(ctx.extend("keyspace"), this.keyspace);
            ConfigUtils.notEmpty(ctx.extend("seeds"), this.seeds);

            final ReadWriteThreadPools pools = ReadWriteThreadPools.config()
                    .readThreads(readThreads).readQueueSize(readQueueSize)
                    .writeThreads(writeThreads).writeQueueSize(writeQueueSize)
                    .build();

            return new HeroicBackend(id, reporter, pools, keyspace, seeds,
                    maxConnectionsPerHost);
        }

        @Override
        protected String defaultGroup() {
            return "heroic";
        }
    }

    private static final ColumnFamily<MetricsRowKey, Integer> METRICS_CF = new ColumnFamily<MetricsRowKey, Integer>(
            "metrics", MetricsRowKeySerializer.get(), IntegerSerializer.get());

    private final BackendReporter reporter;
    private final ReadWriteThreadPools pools;

    public HeroicBackend(String id, BackendReporter reporter,
            ReadWriteThreadPools pools, String keyspace, String seeds,
            int maxConnectionsPerHost) {
        super(id, keyspace, seeds, maxConnectionsPerHost);

        this.reporter = reporter;
        this.pools = pools;
    }

    private static final ColumnFamily<Integer, String> CQL3_CF = ColumnFamily
            .newColumnFamily("Cql3CF", IntegerSerializer.get(),
                    StringSerializer.get());

    private static final String INSERT_METRICS_CQL = "INSERT INTO metrics (metric_key, data_timestamp_offset, data_value) VALUES (?, ?, ?)";

    @Override
    public Callback<WriteBatchResult> write(WriteMetric write) {
        final Collection<WriteMetric> writes = new ArrayList<WriteMetric>();
        writes.add(write);
        return write(writes);
    }

    @Override
    public Callback<WriteBatchResult> write(final Collection<WriteMetric> writes) {
        final Keyspace keyspace = keyspace();

        if (keyspace == null)
            return new CancelledCallback<>(CancelReason.BACKEND_DISABLED);

        final MutationBatch mutation = keyspace.prepareMutationBatch()
                .setConsistencyLevel(ConsistencyLevel.CL_ANY);

        final Map<MetricsRowKey, ColumnListMutation<Integer>> batches = new HashMap<MetricsRowKey, ColumnListMutation<Integer>>();

        for (final WriteMetric write : writes) {
            for (final DataPoint d : write.getData()) {
                final long base = MetricsRowKeySerializer.getBaseTimestamp(d
                        .getTimestamp());
                final MetricsRowKey rowKey = new MetricsRowKey(
                        write.getSeries(), base);

                ColumnListMutation<Integer> m = batches.get(rowKey);

                if (m == null) {
                    m = mutation.withRow(METRICS_CF, rowKey);
                    batches.put(rowKey, m);
                }

                m.putColumn(MetricsRowKeySerializer.calculateColumnKey(d
                        .getTimestamp()), d.getValue());
            }
        }

        final Callback.Resolver<WriteBatchResult> resolver = new Callback.Resolver<WriteBatchResult>() {
            @Override
            public WriteBatchResult resolve() throws Exception {
                mutation.execute();
                return new WriteBatchResult(true, 1);
            }
        };

        return ConcurrentCallback.newResolve(pools.write(), resolver).register(
                reporter.reportWriteBatch());
    }

    /**
     * CQL3 implementation for insertions.
     *
     * TODO: I cannot figure out how to get batch insertions to work. Until
     * then, THIS IS NOT an option because it will murder performance in its
     * sleep and steal its cookies.
     *
     * @param rowKey
     * @param datapoints
     * @return
     */
    @SuppressWarnings("unused")
    private Callback<Integer> writeCQL(final MetricsRowKey rowKey,
            final List<DataPoint> datapoints) {
        final Keyspace keyspace = keyspace();

        if (keyspace == null)
            return new CancelledCallback<Integer>(CancelReason.BACKEND_DISABLED);

        return ConcurrentCallback.newResolve(pools.read(),
                new Callback.Resolver<Integer>() {
                    @Override
                    public Integer resolve() throws Exception {
                        for (final DataPoint d : datapoints) {
                            keyspace.prepareQuery(CQL3_CF)
                                    .withCql(INSERT_METRICS_CQL)
                                    .asPreparedStatement()
                                    .withByteBufferValue(rowKey,
                                            MetricsRowKeySerializer.get())
                                    .withByteBufferValue(
                                            MetricsRowKeySerializer
                                                    .calculateColumnKey(d
                                                            .getTimestamp()),
                                            IntegerSerializer.get())
                                    .withByteBufferValue(d.getValue(),
                                            DoubleSerializer.get()).execute();
                        }

                        return datapoints.size();
                    }
                });
    }

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
            final DateRange range) {
        final Keyspace keyspace = keyspace();

        if (keyspace == null)
            return new CancelledCallback<FetchData>(
                    CancelReason.BACKEND_DISABLED);

        final DateRange newRange = range.modify(base, base
                + MetricsRowKey.MAX_WIDTH - 1);

        if (newRange.isEmpty())
            return null;

        final MetricsRowKey rowKey = new MetricsRowKey(series, base);

        final int start = MetricsRowKeySerializer.calculateColumnKey(newRange
                .getStart());
        final int end = MetricsRowKeySerializer.calculateColumnKey(newRange
                .getEnd());
        final ByteBufferRange columnRange = new RangeBuilder().setStart(start)
                .setEnd(end).build();

        final RowQuery<MetricsRowKey, Integer> dataQuery = keyspace
                .prepareQuery(METRICS_CF).getRow(rowKey).autoPaginate(true)
                .withColumnRange(columnRange);

        final Callback.Resolver<FetchData> resolver = new Callback.Resolver<FetchData>() {
            @Override
            public FetchData resolve() throws Exception {
                final OperationResult<ColumnList<Integer>> result = dataQuery
                        .execute();
                final List<DataPoint> datapoints = rowKey
                        .buildDataPoints(result.getResult());
                return new FetchData(series, datapoints);
            }
        };

        return ConcurrentCallback.newResolve(pools.read(), resolver);
    }

    @Override
    public Callback<Long> getColumnCount(Series series, DateRange range) {
        return new FailedCallback<Long>(new Exception("not implemented"));
    }

    @Override
    public void stop() {
        try {
            super.stop();
        } catch (final Exception e) {
            log.error("Failed to stop", e);
        }

        pools.stop();
    }

    private static List<Long> buildBases(DateRange range) {
        final List<Long> bases = new ArrayList<Long>();

        final long start = MetricsRowKeySerializer.getBaseTimestamp(range
                .getStart());
        final long end = MetricsRowKeySerializer.getBaseTimestamp(range
                .getEnd());

        for (long i = start; i <= end; i += MetricsRowKey.MAX_WIDTH) {
            bases.add(i);
        }

        return bases;
    }

    @Override
    public Iterable<BackendEntry> listEntries() {
        final Keyspace keyspace = keyspace();

        if (keyspace == null)
            throw new IllegalStateException("Backend is not ready");

        final OperationResult<Rows<MetricsRowKey, Integer>> result;

        try {
            result = keyspace.prepareQuery(METRICS_CF).getAllRows().execute();
        } catch (final ConnectionException e) {
            throw new RuntimeException("Request failed", e);
        }

        return new Iterable<BackendEntry>() {
            @Override
            public Iterator<BackendEntry> iterator() {
                final Iterator<Row<MetricsRowKey, Integer>> iterator = result
                        .getResult().iterator();

                return new Iterator<BackendEntry>() {
                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    public BackendEntry next() {
                        final Row<MetricsRowKey, Integer> entry = iterator
                                .next();
                        final MetricsRowKey rowKey = entry.getKey();
                        final Series series = rowKey.getSeries();

                        final List<DataPoint> dataPoints = rowKey
                                .buildDataPoints(entry.getColumns());

                        return new BackendEntry(series, dataPoints);
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }
}
