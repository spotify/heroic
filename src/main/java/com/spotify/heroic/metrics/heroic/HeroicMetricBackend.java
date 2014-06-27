package com.spotify.heroic.metrics.heroic;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.serializers.DoubleSerializer;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.util.RangeBuilder;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.CancelledCallback;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.async.FailedCallback;
import com.spotify.heroic.concurrrency.ReadWriteThreadPools;
import com.spotify.heroic.metadata.MetadataBackendManager;
import com.spotify.heroic.metadata.model.TimeSerieQuery;
import com.spotify.heroic.metrics.MetricBackend;
import com.spotify.heroic.metrics.cassandra.CassandraMetricBackend;
import com.spotify.heroic.metrics.model.FetchDataPoints;
import com.spotify.heroic.metrics.model.FetchDataPoints.Result;
import com.spotify.heroic.metrics.model.FindTimeSeries;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.TimeSerie;
import com.spotify.heroic.model.WriteEntry;
import com.spotify.heroic.model.WriteResponse;
import com.spotify.heroic.statistics.MetricBackendReporter;
import com.spotify.heroic.yaml.Utils;
import com.spotify.heroic.yaml.ValidationException;

/**
 * MetricBackend for Heroic cassandra datastore.
 */
@ToString()
@Slf4j
public class HeroicMetricBackend extends CassandraMetricBackend implements
MetricBackend {
    public static class YAML extends MetricBackend.YAML {
        public static final String TYPE = "!heroic-backend";

        /**
         * Cassandra seed nodes.
         */
        @Getter
        @Setter
        private String seeds;

        /**
         * Cassandra keyspace for heroic.
         */
        @Setter
        private String keyspace = "heroic";

        /**
         *
         */
        @Getter
        @Setter
        private Map<String, String> tags;

        /**
         * Max connections per host in the cassandra cluster.
         */
        @Getter
        @Setter
        private int maxConnectionsPerHost = 20;

        /**
         * Threads dedicated to asynchronous request handling.
         */
        @Getter
        @Setter
        private int readThreads = 20;

        @Getter
        @Setter
        private int readQueueSize = 40;

        /**
         * Threads dedicated to asynchronous request handling.
         */
        @Getter
        @Setter
        private int writeThreads = 20;

        @Getter
        @Setter
        private int writeQueueSize = 1000;

        @Override
        public MetricBackend buildDelegate(String context,
                MetricBackendReporter reporter) throws ValidationException {
            Utils.notEmpty(context + ".keyspace", this.keyspace);
            Utils.notEmpty(context + ".seeds", this.seeds);

            final Map<String, String> attributes = Utils.toMap(context,
                    this.tags);

            final ReadWriteThreadPools pools = ReadWriteThreadPools.config()
                    .readThreads(readThreads).readQueueSize(readQueueSize)
                    .writeThreads(writeThreads).writeQueueSize(writeQueueSize)
                    .build();

            return new HeroicMetricBackend(reporter, pools, keyspace, seeds,
                    maxConnectionsPerHost, attributes);
        }
    }

    private static final ColumnFamily<MetricsRowKey, Long> METRICS_CF = new ColumnFamily<MetricsRowKey, Long>(
            "metrics", MetricsRowKeySerializer.get(), LongSerializer.get());

    private final MetricBackendReporter reporter;
    private final ReadWriteThreadPools pools;

    @Inject
    private MetadataBackendManager metadata;

    public HeroicMetricBackend(MetricBackendReporter reporter,
            ReadWriteThreadPools pools, String keyspace, String seeds,
            int maxConnectionsPerHost, Map<String, String> tags) {
        super(keyspace, seeds, maxConnectionsPerHost, tags);

        this.reporter = reporter;
        this.pools = pools;
    }

    private static final ColumnFamily<Integer, String> CQL3_CF = ColumnFamily
            .newColumnFamily("Cql3CF", IntegerSerializer.get(),
                    StringSerializer.get());

    private static final String INSERT_METRICS_CQL = "INSERT INTO metrics (metric_key, data_timestamp, data_value) VALUES (?, ?, ?)";

    @Override
    public Callback<WriteResponse> write(WriteEntry write) {
        final Collection<WriteEntry> writes = new ArrayList<WriteEntry>();
        writes.add(write);
        return write(writes);
    }

    @Override
    public Callback<WriteResponse> write(final Collection<WriteEntry> writes) {
        final Keyspace keyspace = keyspace();

        if (keyspace == null)
            return new CancelledCallback<WriteResponse>(
                    CancelReason.BACKEND_DISABLED);

        final MutationBatch mutation = keyspace.prepareMutationBatch()
                .setConsistencyLevel(ConsistencyLevel.CL_ANY);

        final Map<MetricsRowKey, ColumnListMutation<Long>> batches = new HashMap<MetricsRowKey, ColumnListMutation<Long>>();

        for (final WriteEntry write : writes) {
            for (final DataPoint d : write.getData()) {
                final long base = buildBase(d.getTimestamp());
                final MetricsRowKey rowKey = new MetricsRowKey(
                        write.getTimeSerie(), base);

                ColumnListMutation<Long> m = batches.get(rowKey);

                if (m == null) {
                    m = mutation.withRow(METRICS_CF, rowKey);
                    batches.put(rowKey, m);
                }

                m.putColumn(d.getTimestamp(), d.getValue());
            }
        }

        final int count = writes.size();

        return ConcurrentCallback.newResolve(pools.write(),
                new Callback.Resolver<WriteResponse>() {
            @Override
            public WriteResponse resolve() throws Exception {
                mutation.execute();
                return new WriteResponse(1, 0, 0);
            }
        }).register(reporter.reportWriteBatch());
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
                            .withByteBufferValue(d.getTimestamp(),
                                    LongSerializer.get())
                                    .withByteBufferValue(d.getValue(),
                                            DoubleSerializer.get()).execute();
                }

                return datapoints.size();
            }
        });
    }

    @Override
    public List<Callback<FetchDataPoints.Result>> query(
            final TimeSerie timeSerie, final DateRange range) {
        final List<Callback<FetchDataPoints.Result>> queries = new ArrayList<Callback<FetchDataPoints.Result>>();

        for (long base : buildBases(range)) {
            final Callback<Result> partial = buildQuery(timeSerie, base, range);

            if (partial == null)
                continue;

            queries.add(partial);
        }

        return queries;
    }

    private Callback<FetchDataPoints.Result> buildQuery(
            final TimeSerie timeSerie, long base, final DateRange range) {
        final Keyspace keyspace = keyspace();

        if (keyspace == null)
            return new CancelledCallback<FetchDataPoints.Result>(
                    CancelReason.BACKEND_DISABLED);

        final DateRange newRange = range.modify(base, base
                + MetricsRowKey.MAX_WIDTH - 1);

        if (newRange.isEmpty())
            return null;

        final MetricsRowKey key = new MetricsRowKey(timeSerie, base);

        final RowQuery<MetricsRowKey, Long> dataQuery = keyspace
                .prepareQuery(METRICS_CF)
                .getRow(key)
                .autoPaginate(true)
                .withColumnRange(
                        new RangeBuilder().setStart(newRange.getStart())
                        .setEnd(newRange.getEnd()).build());

        return ConcurrentCallback.newResolve(pools.read(),
                new Callback.Resolver<FetchDataPoints.Result>() {
            @Override
            public Result resolve() throws Exception {
                final OperationResult<ColumnList<Long>> result = dataQuery
                        .execute();
                final List<DataPoint> datapoints = buildDataPoints(key,
                        result);
                return new FetchDataPoints.Result(datapoints, timeSerie);
            }
        });
    }

    @Override
    public Callback<Set<TimeSerie>> getAllTimeSeries() {
        return new FailedCallback<Set<TimeSerie>>(new Exception(
                "not implemented"));
    }

    @Override
    public Callback<Long> getColumnCount(TimeSerie timeSerie, DateRange range) {
        return new FailedCallback<Long>(new Exception("not implemented"));
    }

    @Override
    public void stop() {
        try {
            super.stop();
        } catch (Exception e) {
            log.error("Failed to stop", e);
        }

        pools.stop();
    }

    private static List<DataPoint> buildDataPoints(final MetricsRowKey key,
            final OperationResult<ColumnList<Long>> result) {
        final List<DataPoint> datapoints = new ArrayList<DataPoint>();

        for (final Column<Long> column : result.getResult()) {
            datapoints.add(new DataPoint(column.getName(), column
                    .getDoubleValue()));
        }

        return datapoints;
    }

    private static long buildBase(long timestamp) {
        return timestamp - timestamp % MetricsRowKey.MAX_WIDTH;
    }

    private static List<Long> buildBases(DateRange range) {
        final List<Long> bases = new ArrayList<Long>();

        final long start = range.getStart() - range.getStart()
                % MetricsRowKey.MAX_WIDTH;
        final long end = range.getEnd() - range.getEnd()
                % MetricsRowKey.MAX_WIDTH + MetricsRowKey.MAX_WIDTH;

        for (long i = start; i < end; i += MetricsRowKey.MAX_WIDTH) {
            bases.add(i);
        }

        return bases;
    }
}
