package com.spotify.heroic.metrics.kairosdb;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
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
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.AllRowsQuery;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.netflix.astyanax.util.RangeBuilder;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.backend.QueryException;
import com.spotify.heroic.injection.Startable;
import com.spotify.heroic.metrics.MetricBackend;
import com.spotify.heroic.metrics.model.FetchDataPoints;
import com.spotify.heroic.metrics.model.FetchDataPoints.Result;
import com.spotify.heroic.metrics.model.FindTimeSeries;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.TimeSerie;
import com.spotify.heroic.statistics.MetricBackendReporter;
import com.spotify.heroic.yaml.Utils;
import com.spotify.heroic.yaml.ValidationException;

/**
 * The data access layer for accessing KairosDB schema in Cassandra.
 * 
 * @author mehrdad
 */
@RequiredArgsConstructor
@Slf4j
public class KairosMetricBackend implements MetricBackend, Startable {
    private static final String COUNT_CQL = "SELECT count(*) FROM data_points WHERE key = ? AND "
            + "column1 > ? AND column1 < ?";

    private final class RowCountTransformer implements Callback.Resolver<Long> {
        private final DateRange range;
        private final DataPointsRowKey row;

        private RowCountTransformer(DateRange range, DataPointsRowKey row) {
            this.range = range;
            this.row = row;
        }

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

    public static class YAML implements MetricBackend.YAML {
        public static final String TYPE = "!kairosdb-backend";

        /**
         * Cassandra seed nodes.
         */
        @Getter
        @Setter
        private String seeds;

        /**
         * Cassandra keyspace for kairosdb.
         */
        @Setter
        private String keyspace = "kairosdb";

        /**
         * Attributes passed into the Astyanax driver for configuration.
         */
        @Getter
        @Setter
        private Map<String, String> attributes;

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
                    this.attributes);
            final Executor executor = Executors.newFixedThreadPool(threads);
            return new KairosMetricBackend(reporter, executor, keyspace, seeds,
                    maxConnectionsPerHost, attributes);
        }
    }

    private static final ColumnFamily<DataPointsRowKey, Integer> DATA_POINTS_CF = new ColumnFamily<DataPointsRowKey, Integer>(
            "data_points", DataPointsRowKey.Serializer.get(),
            IntegerSerializer.get());

    private final ColumnFamily<String, DataPointsRowKey> ROW_KEY_INDEX_CF = new ColumnFamily<>(
            "row_key_index", StringSerializer.get(),
            DataPointsRowKey.Serializer.get());

    private final MetricBackendReporter reporter;
    private final Executor executor;
    private final String keyspaceName;
    private final String seeds;
    private final int maxConnectionsPerHost;
    private final Map<String, String> backendTags;

    private AstyanaxContext<Keyspace> context;
    private Keyspace keyspace;

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
            final TimeSerie timeSerie, long base, DateRange queryRange) {
        final DataPointsRowKey rowKey = new DataPointsRowKey(
                timeSerie.getKey(), base, timeSerie.getTags());
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

        return ConcurrentCallback.newResolve(executor,
                new Callback.Resolver<FetchDataPoints.Result>() {
                    @Override
                    public Result resolve() throws Exception {
                        final OperationResult<ColumnList<Integer>> result = dataQuery
                                .execute();
                        final List<DataPoint> datapoints = buildDataPoints(
                                rowKey, result);
                        return new FetchDataPoints.Result(datapoints, timeSerie);
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
    public Callback<Long> getColumnCount(final TimeSerie timeSerie,
            final DateRange range) {
        final List<Callback<Long>> callbacks = new ArrayList<Callback<Long>>();

        for (long base : buildBases(range)) {
            final DataPointsRowKey row = new DataPointsRowKey(
                    timeSerie.getKey(), base, timeSerie.getTags());
            callbacks.add(ConcurrentCallback.newResolve(executor,
                    new RowCountTransformer(range, row)));
        }

        return ConcurrentCallback.newReduce(callbacks,
                new Callback.Reducer<Long, Long>() {
                    @Override
                    public Long resolved(Collection<Long> results,
                            Collection<Exception> errors,
                            Collection<CancelReason> cancelled)
                            throws Exception {
                        long value = 0;

                        for (long result : results) {
                            value += result;
                        }

                        return value;
                    }
                });
    }

    @Override
    public Callback<FindTimeSeries.Result> findTimeSeries(
            final FindTimeSeries query) throws QueryException {
        final String key = query.getKey();
        final DateRange range = query.getRange();
        final Map<String, String> filter = query.getFilter();
        final List<String> groupBy = query.getGroupBy();

        final DataPointsRowKey startKey = rowKeyStart(range.start(), key);
        final DataPointsRowKey endKey = rowKeyEnd(range.end(), key);

        final RowQuery<String, DataPointsRowKey> rowQuery = keyspace
                .prepareQuery(ROW_KEY_INDEX_CF)
                .getRow(key)
                .autoPaginate(true)
                .withColumnRange(
                        new RangeBuilder()
                                .setStart(startKey,
                                        DataPointsRowKey.Serializer.get())
                                .setEnd(endKey,
                                        DataPointsRowKey.Serializer.get())
                                .build());

        return ConcurrentCallback.newResolve(executor,
                new Callback.Resolver<FindTimeSeries.Result>() {
                    @Override
                    public FindTimeSeries.Result resolve() throws Exception {
                        final OperationResult<ColumnList<DataPointsRowKey>> result = rowQuery
                                .execute();

                        final Map<TimeSerie, Set<TimeSerie>> rowGroups = new HashMap<TimeSerie, Set<TimeSerie>>();

                        final ColumnList<DataPointsRowKey> columns = result
                                .getResult();

                        for (final Column<DataPointsRowKey> column : columns) {
                            final DataPointsRowKey rowKey = column.getName();
                            final Map<String, String> rowTags = rowKey
                                    .getTags();

                            if (!matchingTags(rowTags, backendTags, filter)) {
                                continue;
                            }

                            final Map<String, String> tags = new HashMap<String, String>(
                                    filter);

                            if (groupBy != null) {
                                for (final String group : groupBy) {
                                    tags.put(group, rowTags.get(group));
                                }
                            }

                            final TimeSerie timeSerie = new TimeSerie(key, tags);

                            Set<TimeSerie> timeSeries = rowGroups
                                    .get(timeSerie);

                            if (timeSeries == null) {
                                timeSeries = new HashSet<TimeSerie>();
                                rowGroups.put(timeSerie, timeSeries);
                            }

                            timeSeries.add(new TimeSerie(
                                    rowKey.getMetricName(), rowKey.getTags()));
                        }

                        return new FindTimeSeries.Result(rowGroups,
                                KairosMetricBackend.this);
                    }
                });
    }

    @Override
    public Callback<Set<TimeSerie>> getAllTimeSeries() {
        final AllRowsQuery<String, DataPointsRowKey> rowQuery = keyspace
                .prepareQuery(ROW_KEY_INDEX_CF).getAllRows();

        return ConcurrentCallback.newResolve(executor,
                new Callback.Resolver<Set<TimeSerie>>() {
                    @Override
                    public Set<TimeSerie> resolve() throws Exception {
                        final Set<TimeSerie> timeSeries = new HashSet<TimeSerie>();
                        final OperationResult<Rows<String, DataPointsRowKey>> result = rowQuery
                                .execute();

                        final Rows<String, DataPointsRowKey> rows = result
                                .getResult();

                        for (final Row<String, DataPointsRowKey> row : rows) {
                            for (final Column<DataPointsRowKey> column : row
                                    .getColumns()) {
                                final DataPointsRowKey rowKey = column
                                        .getName();
                                timeSeries.add(new TimeSerie(rowKey
                                        .getMetricName(), rowKey.getTags()));
                            }
                        }

                        return timeSeries;
                    }
                });
    }

    private static boolean matchingTags(Map<String, String> tags,
            Map<String, String> backendTags, Map<String, String> queryTags) {
        // query not specified.
        if (queryTags != null) {
            // match the row tags with the query tags.
            for (final Map.Entry<String, String> entry : queryTags.entrySet()) {
                // check tags for the actual row.
                final String tagValue = tags.get(entry.getKey());

                if (tagValue == null || !tagValue.equals(entry.getValue())) {
                    return false;
                }

                // check built-in attributes for this datasource.
                final String attributeValue = backendTags.get(entry.getKey());

                // check built-in attribute for this backend.
                if (attributeValue != null
                        && !attributeValue.equals(entry.getValue())) {
                    return false;
                }
            }
        }

        return true;
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

    private static DataPointsRowKey rowKeyStart(long start, String key) {
        final long timeBucket = DataPointsRowKey.getTimeBucket(start);
        return new DataPointsRowKey(key, timeBucket);
    }

    private static DataPointsRowKey rowKeyEnd(long end, String key) {
        final long timeBucket = DataPointsRowKey.getTimeBucket(end);
        return new DataPointsRowKey(key, timeBucket + 1);
    }
}
