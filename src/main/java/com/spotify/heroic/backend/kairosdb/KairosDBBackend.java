package com.spotify.heroic.backend.kairosdb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import lombok.Getter;
import lombok.Setter;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
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
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.backend.Backend;
import com.spotify.heroic.backend.MetricBackend;
import com.spotify.heroic.backend.QueryException;
import com.spotify.heroic.backend.model.FetchDataPoints;
import com.spotify.heroic.backend.model.FetchDataPoints.Result;
import com.spotify.heroic.backend.model.FindRows;
import com.spotify.heroic.backend.model.GetAllRowsResult;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.TimeSerie;
import com.spotify.heroic.yaml.Utils;
import com.spotify.heroic.yaml.ValidationException;

/**
 * The data access layer for accessing KairosDB schema in Cassandra.
 * 
 * @author mehrdad
 */
public class KairosDBBackend implements MetricBackend {
    public static final String QUERY = "query";
    public static final String GET_COLUMN_COUNT = "get-column-count";
    private static final String GET_ALL_ROWS = "get-all-rows";
    private static final String FIND_ROW_GROUPS = "find-row-groups";

    private static final String CF_DATA_POINTS_NAME = "data_points";
    private static final String CF_ROW_KEY_INDEX = "row_key_index";

    private static final String COUNT_CQL = "SELECT count(*) FROM data_points WHERE key = ? AND "
            + "column1 > ? AND column1 < ?";

    public static class YAML implements Backend.YAML {
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
        @Getter
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
        public Backend build(String context, MetricRegistry registry)
                throws ValidationException {
            Utils.notEmpty(context + ".keyspace", this.keyspace);
            Utils.notEmpty(context + ".seeds", this.seeds);
            final Map<String, String> attributes = Utils.toMap(context,
                    this.attributes);
            final Executor executor = Executors.newFixedThreadPool(threads);
            return new KairosDBBackend(registry, executor, keyspace, seeds,
                    maxConnectionsPerHost, attributes);
        }
    }

    private final Executor executor;
    private final Map<String, String> backendTags;
    private final Keyspace keyspace;
    private final ColumnFamily<DataPointsRowKey, Integer> dataPoints;
    private final ColumnFamily<String, DataPointsRowKey> rowKeyIndex;
    ColumnFamily<Integer, String> CQL3_CF = ColumnFamily.newColumnFamily(
            "Cql3CF", IntegerSerializer.get(), StringSerializer.get());

    private final AstyanaxContext<Keyspace> ctx;

    @SuppressWarnings("unused")
    private final Timer queryTimer;
    @SuppressWarnings("unused")
    private final Timer findRowGroupsTimer;
    @SuppressWarnings("unused")
    private final Timer getAllRowsTimer;
    @SuppressWarnings("unused")
    private final Timer getColumnCountTimer;

    public KairosDBBackend(MetricRegistry registry, Executor executor,
            String keyspace, String seeds, int maxConnectionsPerHost,
            Map<String, String> backendTags) {

        final AstyanaxConfiguration config = new AstyanaxConfigurationImpl()
                .setCqlVersion("3.0.0").setTargetCassandraVersion("2.0");

        ctx = new AstyanaxContext.Builder()
                .withConnectionPoolConfiguration(
                        new ConnectionPoolConfigurationImpl(
                                "HeroicConnectionPool").setPort(9160)
                                .setMaxConnsPerHost(maxConnectionsPerHost)
                                .setSeeds(seeds)).forKeyspace(keyspace)
                .withAstyanaxConfiguration(config)
                .buildKeyspace(ThriftFamilyFactory.getInstance());

        ctx.start();

        this.executor = executor;

        this.dataPoints = new ColumnFamily<DataPointsRowKey, Integer>(
                CF_DATA_POINTS_NAME, DataPointsRowKey.Serializer.get(),
                IntegerSerializer.get());

        this.rowKeyIndex = new ColumnFamily<>(CF_ROW_KEY_INDEX,
                StringSerializer.get(), DataPointsRowKey.Serializer.get());

        this.keyspace = ctx.getClient();

        this.backendTags = backendTags;

        /* setup metric timers */
        this.queryTimer = registry.timer(MetricRegistry.name("heroic",
                "kairosdb", QUERY, backendTags.toString()));
        this.findRowGroupsTimer = registry.timer(MetricRegistry.name("heroic",
                "kairosdb", FIND_ROW_GROUPS, backendTags.toString()));
        this.getAllRowsTimer = registry.timer(MetricRegistry.name("heroic",
                "kairosdb", GET_ALL_ROWS, backendTags.toString()));
        this.getColumnCountTimer = registry.timer(MetricRegistry.name("heroic",
                "kairosdb", GET_COLUMN_COUNT, backendTags.toString()));
    }

    @Override
    public List<Callback<FetchDataPoints.Result>> query(
            FetchDataPoints fetchDataPointsQuery) {
        final List<Callback<FetchDataPoints.Result>> queries = new ArrayList<Callback<FetchDataPoints.Result>>();

        final List<DataPointsRowKey> rows = fetchDataPointsQuery.getRows();
        final DateRange range = fetchDataPointsQuery.getRange();

        for (final DataPointsRowKey rowKey : rows) {
            queries.add(buildQuery(rowKey, range));
        }

        return queries;
    }

    private Callback<FetchDataPoints.Result> buildQuery(
            final DataPointsRowKey rowKey, DateRange range) {
        final long timestamp = rowKey.getTimestamp();
        final long startTime = DataPoint.Name.toStartTimeStamp(range.start(),
                timestamp);
        final long endTime = DataPoint.Name.toEndTimeStamp(range.end(),
                timestamp);

        final RowQuery<DataPointsRowKey, Integer> dataQuery = keyspace
                .prepareQuery(dataPoints)
                .getRow(rowKey)
                .autoPaginate(true)
                .withColumnRange(
                        new RangeBuilder()
                                .setStart((int) startTime,
                                        IntegerSerializer.get())
                                .setEnd((int) endTime, IntegerSerializer.get())
                                .build());

        return ConcurrentCallback.newResolve(executor, new Callback.Resolver<FetchDataPoints.Result>() {
            @Override
            public Result run() throws Exception {
                final OperationResult<ColumnList<Integer>> result = dataQuery.execute();
                final List<DataPoint> datapoints = buildDataPoints(rowKey, result);
                return new FetchDataPoints.Result(datapoints, rowKey);
            }

            private List<DataPoint> buildDataPoints(final DataPointsRowKey rowKey,
                    final OperationResult<ColumnList<Integer>> result) {
                final List<DataPoint> datapoints = new ArrayList<DataPoint>();

                for (final Column<Integer> column : result.getResult()) {
                    datapoints.add(DataPoint.fromColumn(rowKey, column));
                }

                return datapoints;
            }
        });
    }

    @Override
    public Callback<Long> getColumnCount(final DataPointsRowKey row, final DateRange range) {
        return ConcurrentCallback.newResolve(executor, new Callback.Resolver<Long>() {
            @Override
            public Long run() throws Exception {
                final long timestamp = row.getTimestamp();
                final long start = DataPoint.Name.toStartTimeStamp(range.start(),
                        timestamp);
                final long end = DataPoint.Name.toEndTimeStamp(range.end(),
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
        });
    }

    @Override
    public Callback<FindRows.Result> findRows(
            final FindRows query) throws QueryException {
        final String key = query.getKey();
        final DateRange range = query.getRange();
        final Map<String, String> filter = query.getFilter();
        final List<String> groupBy = query.getGroupBy();

        final DataPointsRowKey startKey = rowKeyStart(range.start(), key);
        final DataPointsRowKey endKey = rowKeyEnd(range.end(), key);

        final RowQuery<String, DataPointsRowKey> rowQuery = keyspace
                .prepareQuery(rowKeyIndex)
                .getRow(key)
                .autoPaginate(true)
                .withColumnRange(
                        new RangeBuilder()
                                .setStart(startKey,
                                        DataPointsRowKey.Serializer.get())
                                .setEnd(endKey,
                                        DataPointsRowKey.Serializer.get())
                                .build());

        return ConcurrentCallback.newResolve(executor, new Callback.Resolver<FindRows.Result>() {
            @Override
            public com.spotify.heroic.backend.model.FindRows.Result run()
                    throws Exception {
                final OperationResult<ColumnList<DataPointsRowKey>> result = rowQuery
                        .execute();

                final Map<TimeSerie, List<DataPointsRowKey>> rowGroups = new HashMap<TimeSerie, List<DataPointsRowKey>>();

                final ColumnList<DataPointsRowKey> columns = result.getResult();

                for (final Column<DataPointsRowKey> column : columns) {
                    final DataPointsRowKey rowKey = column.getName();
                    final Map<String, String> rowTags = rowKey.getTags();

                    if (!matchingTags(rowTags, backendTags, filter)) {
                        continue;
                    }

                    final Map<String, String> tags = new HashMap<String, String>(filter);

                    if (groupBy != null) {
                        for (final String group : groupBy) {
                            tags.put(group, rowTags.get(group));
                        }
                    }

                    final TimeSerie timeSerie = new TimeSerie(key, tags);

                    List<DataPointsRowKey> rows = rowGroups.get(timeSerie);

                    if (rows == null) {
                        rows = new ArrayList<DataPointsRowKey>();
                        rowGroups.put(timeSerie, rows);
                    }

                    rows.add(rowKey);
                }

                return new FindRows.Result(rowGroups, KairosDBBackend.this);
            }
        });
    }

    @Override
    public Callback<GetAllRowsResult> getAllRows() {
        final AllRowsQuery<String, DataPointsRowKey> rowQuery = keyspace
                .prepareQuery(rowKeyIndex).getAllRows();

        return ConcurrentCallback.newResolve(executor, new Callback.Resolver<GetAllRowsResult>() {
            @Override
            public GetAllRowsResult run() throws Exception {
                final Map<String, List<DataPointsRowKey>> queryResult = new HashMap<String, List<DataPointsRowKey>>();
                final OperationResult<Rows<String, DataPointsRowKey>> result = rowQuery
                        .execute();

                final Rows<String, DataPointsRowKey> rows = result.getResult();

                for (final Row<String, DataPointsRowKey> row : rows) {
                    final List<DataPointsRowKey> columns = new ArrayList<DataPointsRowKey>(
                            row.getColumns().size());

                    for (final Column<DataPointsRowKey> column : row.getColumns()) {
                        final DataPointsRowKey name = column.getName();
                        columns.add(name);
                    }

                    queryResult.put(row.getKey(), columns);
                }

                return new GetAllRowsResult(queryResult);
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

    private DataPointsRowKey rowKeyStart(long start, String key) {
        final long timeBucket = DataPointsRowKey.getTimeBucket(start);
        return new DataPointsRowKey(key, timeBucket);
    }

    private DataPointsRowKey rowKeyEnd(long end, String key) {
        final long timeBucket = DataPointsRowKey.getTimeBucket(end);
        return new DataPointsRowKey(key, timeBucket + 1);
    }
}
