package com.spotify.heroic.metrics.heroic;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import com.netflix.astyanax.AstyanaxConfiguration;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.serializers.DoubleSerializer;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.netflix.astyanax.util.RangeBuilder;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.async.FailedCallback;
import com.spotify.heroic.injection.Startable;
import com.spotify.heroic.metrics.MetricBackend;
import com.spotify.heroic.metrics.model.FetchDataPoints;
import com.spotify.heroic.metrics.model.FetchDataPoints.Result;
import com.spotify.heroic.metrics.model.FindTimeSeries;
import com.spotify.heroic.metrics.model.WriteResponse;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.TimeSerie;
import com.spotify.heroic.statistics.MetricBackendReporter;
import com.spotify.heroic.yaml.Utils;
import com.spotify.heroic.yaml.ValidationException;

/**
 * MetricBackend for Heroic cassandra datastore.
 */
@RequiredArgsConstructor
@Slf4j
public class HeroicMetricBackend implements MetricBackend, Startable {
    public static class YAML implements MetricBackend.YAML {
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
        private int threads = 20;

        @Override
        public MetricBackend build(String context,
                MetricBackendReporter reporter) throws ValidationException {
            Utils.notEmpty(context + ".keyspace", this.keyspace);
            Utils.notEmpty(context + ".seeds", this.seeds);
            final Map<String, String> attributes = Utils.toMap(context,
                    this.tags);
            final Executor executor = Executors.newFixedThreadPool(threads);
            return new HeroicMetricBackend(reporter, executor, keyspace, seeds,
                    maxConnectionsPerHost, attributes);
        }
    }

    private static final ColumnFamily<DataPointsRowKey, Integer> METRICS_CF = new ColumnFamily<DataPointsRowKey, Integer>(
            "metrics", DataPointsRowKeySerializer.get(),
            IntegerSerializer.get());

    private final MetricBackendReporter reporter;
    private final Executor executor;
    private final String keyspaceName;
    private final String seeds;
    private final int maxConnectionsPerHost;
    private final Map<String, String> backendTags;

    private AstyanaxContext<Keyspace> context;
    private Keyspace keyspace;

    @Override
    public boolean matches(final TimeSerie timeSerie) {
        final Map<String, String> tags = timeSerie.getTags();

        if ((tags == null || tags.isEmpty()) && !backendTags.isEmpty())
            return false;

        for (Map.Entry<String, String> entry : backendTags.entrySet()) {
            if (!tags.get(entry.getKey()).equals(entry.getValue())) {
                return false;
            }
        }

        return true;
    }

    @Override
    public void start() throws Exception {
        log.info("Starting");

        final AstyanaxConfiguration config = new AstyanaxConfigurationImpl()
                .setCqlVersion("3.0.0").setTargetCassandraVersion("2.0");

        context = new AstyanaxContext.Builder()
                .withConnectionPoolConfiguration(
                        new ConnectionPoolConfigurationImpl(
                                "HeroicConnectionPool").setPort(9160)
                                .setMaxConnsPerHost(maxConnectionsPerHost)
                                .setSeeds(seeds)).forKeyspace(keyspaceName)
                .withAstyanaxConfiguration(config)
                .buildKeyspace(ThriftFamilyFactory.getInstance());

        context.start();
        keyspace = context.getClient();
    }

    private static final ColumnFamily<Integer, String> CQL3_CF = ColumnFamily
            .newColumnFamily("Cql3CF", IntegerSerializer.get(),
                    StringSerializer.get());

    private static final String INSERT_METRICS_CQL = "INSERT INTO metrics (metric_key, data_timestamp, data_value) VALUES (?, ?, ?)";

    @Override
    public Callback<WriteResponse> write(final TimeSerie timeSerie,
            final List<DataPoint> datapoints) {
        final Map<Long, List<DataPoint>> batches = buildBatches(datapoints);

        final List<Callback<Integer>> callbacks = new ArrayList<Callback<Integer>>();

        for (final Map.Entry<Long, List<DataPoint>> batch : batches.entrySet()) {
            final long base = batch.getKey();
            final DataPointsRowKey rowKey = new DataPointsRowKey(timeSerie,
                    base);
            callbacks.add(write(rowKey, batch.getValue()));
        }

        return ConcurrentCallback.newReduce(callbacks,
                new Callback.Reducer<Integer, WriteResponse>() {
                    @Override
                    public WriteResponse resolved(Collection<Integer> results,
                            Collection<Exception> errors,
                            Collection<CancelReason> cancelled)
                            throws Exception {
                        for (Exception e : errors) {
                            log.error("Failed to write", e);
                        }

                        return new WriteResponse();
                    }
                });
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
    private Callback<Integer> writeCQL(final DataPointsRowKey rowKey,
            final List<DataPoint> datapoints) {
        return ConcurrentCallback.newResolve(executor,
                new Callback.Resolver<Integer>() {
                    @Override
                    public Integer resolve() throws Exception {
                        for (final DataPoint d : datapoints) {
                            keyspace.prepareQuery(CQL3_CF)
                                    .withCql(INSERT_METRICS_CQL)
                                    .asPreparedStatement()
                                    .withByteBufferValue(rowKey,
                                            DataPointsRowKeySerializer.get())
                                    .withByteBufferValue(d.getTimestamp(),
                                            LongSerializer.get())
                                    .withByteBufferValue(d.getValue(),
                                            DoubleSerializer.get()).execute();
                        }

                        return datapoints.size();
                    }
                });
    }

    /**
     * The thrift batch mutation write function.
     * 
     * Use this since it will help with performance.
     * 
     * @param rowKey
     * @param datapoints
     * @return
     */
    private Callback<Integer> write(final DataPointsRowKey rowKey,
            final List<DataPoint> datapoints) {
        final MutationBatch mutation = keyspace.prepareMutationBatch()
                .setConsistencyLevel(ConsistencyLevel.CL_ANY);

        final ColumnListMutation<Integer> m = mutation.withRow(METRICS_CF,
                rowKey);

        for (final DataPoint d : datapoints) {
            int key = (int) (d.getTimestamp() & DataPointsRowKey.MAX_BITSET);
            m.putColumn(key, d.getValue());
        }

        return ConcurrentCallback.newResolve(executor,
                new Callback.Resolver<Integer>() {
                    @Override
                    public Integer resolve() throws Exception {
                        mutation.execute();
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
        final DateRange newRange = range.modify(base, base
                + DataPointsRowKey.MAX_WIDTH);

        if (newRange.isEmpty())
            return null;

        final DataPointsRowKey key = new DataPointsRowKey(timeSerie, base);

        int start = (int) (newRange.getStart() & DataPointsRowKey.MAX_BITSET);
        int end = (int) (newRange.getEnd() & DataPointsRowKey.MAX_BITSET);

        final RowQuery<DataPointsRowKey, Integer> dataQuery = keyspace
                .prepareQuery(METRICS_CF)
                .getRow(key)
                .autoPaginate(true)
                .withColumnRange(
                        new RangeBuilder()
                                .setStart(start, IntegerSerializer.get())
                                .setEnd(end, IntegerSerializer.get()).build());

        return ConcurrentCallback.newResolve(executor,
                new Callback.Resolver<FetchDataPoints.Result>() {
                    @Override
                    public Result resolve() throws Exception {
                        final OperationResult<ColumnList<Integer>> result = dataQuery
                                .execute();
                        final List<DataPoint> datapoints = buildDataPoints(key,
                                result);
                        return new FetchDataPoints.Result(datapoints, timeSerie);
                    }
                });
    }

    @Override
    public Callback<FindTimeSeries.Result> findTimeSeries(
            final FindTimeSeries query) {
        // TODO: USE METADATA BACKEND.

        /*
         * return ConcurrentCallback.newResolve(executor, new
         * Callback.Resolver<FindTimeSeries.Result>() {
         * 
         * @Override public FindTimeSeries.Result resolve() throws Exception {
         * final OperationResult<ColumnList<DataPointsRowKey>> result = rowQuery
         * .execute();
         * 
         * final Map<TimeSerie, Set<TimeSerie>> rowGroups = new
         * HashMap<TimeSerie, Set<TimeSerie>>();
         * 
         * final ColumnList<DataPointsRowKey> columns = result .getResult();
         * 
         * for (final Column<DataPointsRowKey> column : columns) { final
         * DataPointsRowKey rowKey = column.getName(); final TimeSerie
         * rowTimeSerie = rowKey .getTimeSerie(); final Map<String, String>
         * rowTags = rowTimeSerie .getTags();
         * 
         * final Map<String, String> tags = new HashMap<String, String>(
         * filter);
         * 
         * if (groupBy != null) { for (final String group : groupBy) {
         * tags.put(group, rowTags.get(group)); } }
         * 
         * final TimeSerie timeSerie = rowTimeSerie .withTags(tags);
         * 
         * Set<TimeSerie> timeSeries = rowGroups .get(timeSerie);
         * 
         * if (timeSeries == null) { timeSeries = new HashSet<TimeSerie>();
         * rowGroups.put(timeSerie, timeSeries); }
         * 
         * timeSeries.add(rowTimeSerie); }
         * 
         * return new FindTimeSeries.Result(rowGroups,
         * HeroicMetricBackend.this); } });
         */
        return new FailedCallback<FindTimeSeries.Result>(new Exception(
                "not implemented"));
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

    private static List<DataPoint> buildDataPoints(final DataPointsRowKey key,
            final OperationResult<ColumnList<Integer>> result) {
        final List<DataPoint> datapoints = new ArrayList<DataPoint>();

        for (final Column<Integer> column : result.getResult()) {
            datapoints.add(new DataPoint(key.getBase()
                    + ((long) column.getName()), column.getDoubleValue()));
        }

        return datapoints;
    }

    private static Map<Long, List<DataPoint>> buildBatches(
            final List<DataPoint> datapoints) {
        final Map<Long, List<DataPoint>> batches = new HashMap<Long, List<DataPoint>>();

        for (final DataPoint d : datapoints) {
            final long base = buildBase(d.getTimestamp());
            List<DataPoint> batch = batches.get(base);

            if (batch == null) {
                batch = new ArrayList<DataPoint>();
                batches.put(base, batch);
            }

            batch.add(d);
        }

        return batches;
    }

    private static long buildBase(long timestamp) {
        return timestamp - timestamp % DataPointsRowKey.MAX_WIDTH;
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
}
