package com.spotify.heroic.metrics.heroic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import javax.inject.Inject;

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
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.async.FailedCallback;
import com.spotify.heroic.injection.Startable;
import com.spotify.heroic.metadata.MetadataBackendManager;
import com.spotify.heroic.metadata.model.TimeSerieQuery;
import com.spotify.heroic.metrics.MetricBackend;
import com.spotify.heroic.metrics.model.FetchDataPoints;
import com.spotify.heroic.metrics.model.FetchDataPoints.Result;
import com.spotify.heroic.metrics.model.FindTimeSeries;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.TimeSerie;
import com.spotify.heroic.model.WriteResponse;
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
        private int readThreads = 20;

        /**
         * Threads dedicated to asynchronous request handling.
         */
        @Getter
        @Setter
        private int writeThreads = 20;

        @Override
        public MetricBackend build(String context,
                MetricBackendReporter reporter) throws ValidationException {
            Utils.notEmpty(context + ".keyspace", this.keyspace);
            Utils.notEmpty(context + ".seeds", this.seeds);
            final Map<String, String> attributes = Utils.toMap(context,
                    this.tags);
            final Executor readExecutor = Executors
                    .newFixedThreadPool(readThreads);
            final Executor writeExecutor = Executors
                    .newFixedThreadPool(writeThreads);
            return new HeroicMetricBackend(reporter, readExecutor,
                    writeExecutor, keyspace, seeds,
                    maxConnectionsPerHost, attributes);
        }
    }

    private static final ColumnFamily<MetricsRowKey, Long> METRICS_CF = new ColumnFamily<MetricsRowKey, Long>(
            "metrics", MetricsRowKeySerializer.get(), LongSerializer.get());

    private final MetricBackendReporter reporter;
    private final Executor readExecutor;
    private final Executor writeExecutor;
    private final String keyspaceName;
    private final String seeds;
    private final int maxConnectionsPerHost;
    private final Map<String, String> backendTags;

    private AstyanaxContext<Keyspace> context;
    private Keyspace keyspace;

    @Inject
    private MetadataBackendManager metadata;

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
        final MutationBatch mutation = keyspace.prepareMutationBatch()
                .setConsistencyLevel(ConsistencyLevel.CL_ANY);

        final Map<Long, ColumnListMutation<Long>> batches = new HashMap<Long, ColumnListMutation<Long>>();

        for (final DataPoint d : datapoints) {
            final long base = buildBase(d.getTimestamp());
            ColumnListMutation<Long> m = batches.get(base);

            if (m == null) {
                final MetricsRowKey rowKey = new MetricsRowKey(timeSerie, base);
                m = mutation.withRow(METRICS_CF, rowKey);
                batches.put(base, m);
            }

            m.putColumn(d.getTimestamp(), d.getValue());
        }

        return ConcurrentCallback.newResolve(writeExecutor,
                new Callback.Resolver<WriteResponse>() {
            @Override
            public WriteResponse resolve() throws Exception {
                mutation.execute();
                return new WriteResponse();
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
        return ConcurrentCallback.newResolve(readExecutor,
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

        return ConcurrentCallback.newResolve(readExecutor,
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
    public Callback<FindTimeSeries.Result> findTimeSeries(
            final FindTimeSeries query) {
        final Map<String, String> filter = query.getFilter();
        final List<String> groupBy = query.getGroupBy();

        return metadata
                .findTimeSeries(
                        new TimeSerieQuery(query.getKey(), filter, null))
                        .transform(
                                new Callback.Transformer<com.spotify.heroic.metadata.model.FindTimeSeries, FindTimeSeries.Result>() {
                                    @Override
                                    public FindTimeSeries.Result transform(
                                            com.spotify.heroic.metadata.model.FindTimeSeries result)
                                                    throws Exception {
                                        final Map<TimeSerie, Set<TimeSerie>> groups = new HashMap<TimeSerie, Set<TimeSerie>>();

                                        for (final TimeSerie timeSerie : result
                                                .getTimeSeries()) {
                                            final Map<String, String> tags = new HashMap<String, String>(
                                                    filter);

                                            if (groupBy != null) {
                                                for (final String group : groupBy) {
                                                    tags.put(group, timeSerie.getTags()
                                                            .get(group));
                                                }
                                            }

                                            final TimeSerie key = timeSerie
                                                    .withTags(tags);

                                            Set<TimeSerie> group = groups.get(key);

                                            if (group == null) {
                                                group = new HashSet<TimeSerie>();
                                                groups.put(key, group);
                                            }

                                            group.add(timeSerie);
                                        }

                                        return new FindTimeSeries.Result(groups,
                                                HeroicMetricBackend.this);
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

    private static List<DataPoint> buildDataPoints(final MetricsRowKey key,
            final OperationResult<ColumnList<Long>> result) {
        final List<DataPoint> datapoints = new ArrayList<DataPoint>();

        for (final Column<Long> column : result.getResult()) {
            datapoints.add(new DataPoint(column.getName(), column
                    .getDoubleValue(), Float.NaN));
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
