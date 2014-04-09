package com.spotify.heroic.backend.kairosdb;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.netflix.astyanax.AstyanaxConfiguration;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.AllRowsQuery;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.netflix.astyanax.util.RangeBuilder;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CallbackRunnable;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.backend.Backend;
import com.spotify.heroic.backend.MetricBackend;
import com.spotify.heroic.backend.QueryException;
import com.spotify.heroic.query.DateRange;
import com.spotify.heroic.yaml.Utils;
import com.spotify.heroic.yaml.ValidationException;

@Slf4j
public class KairosDBBackend implements MetricBackend {

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

    private final Timer queryTimer;
    private final Timer findRowsTimer;
    private final Timer findRowGroupsTimer;
    private final Timer getAllRowsTimer;

    private static final String CF_DATA_POINTS_NAME = "data_points";
    private static final String CF_ROW_KEY_INDEX = "row_key_index";

    public KairosDBBackend(MetricRegistry registry, Executor executor,
            String keyspace, String seeds, int maxConnectionsPerHost,
            Map<String, String> backendTags) {

        final AstyanaxConfiguration config = new AstyanaxConfigurationImpl()
                .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
                .setConnectionPoolType(ConnectionPoolType.TOKEN_AWARE);

        final AstyanaxContext<Keyspace> ctx = new AstyanaxContext.Builder()
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
                "kairosdb", "query", backendTags.toString()));
        this.findRowGroupsTimer = registry.timer(MetricRegistry.name("heroic",
                "kairosdb", "find-row-groups", backendTags.toString()));
        this.findRowsTimer = registry.timer(MetricRegistry.name("heroic",
                "kairosdb", "find-rows", backendTags.toString()));
        this.getAllRowsTimer = registry.timer(MetricRegistry.name("heroic",
                "kairosdb", "get-all-rows", backendTags.toString()));
    }

    @Override
    public List<Callback<DataPointsResult>> query(List<DataPointsRowKey> rows,
            DateRange range) throws QueryException {
        final long start = range.start().getTime();
        final long end = range.end().getTime();

        final List<Callback<DataPointsResult>> queries = new ArrayList<Callback<DataPointsResult>>();

        for (final DataPointsRowKey rowKey : rows) {
            final Callback<DataPointsResult> callback = buildQuery(rowKey,
                    start, end);

            final Timer.Context context = queryTimer.time();

            callback.register(new Callback.Ended() {
                @Override
                public void ended() throws Exception {
                    context.stop();
                }
            });

            queries.add(callback);
        }

        return queries;
    }

    private final class QueryCallbackRunnable extends
            CallbackRunnable<DataPointsResult> {
        private final RowQuery<DataPointsRowKey, Integer> dataQuery;
        private final DataPointsRowKey rowKey;

        private QueryCallbackRunnable(Callback<DataPointsResult> callback,
                Timer timer, RowQuery<DataPointsRowKey, Integer> dataQuery,
                DataPointsRowKey rowKey) {
            super(callback, timer);
            this.dataQuery = dataQuery;
            this.rowKey = rowKey;
        }

        @Override
        public DataPointsResult execute() throws Exception {
            final OperationResult<ColumnList<Integer>> result = dataQuery
                    .execute();
            final List<DataPoint> datapoints = buildDataPoints(rowKey, result);

            return new DataPointsResult(datapoints, rowKey);
        }

        private List<DataPoint> buildDataPoints(final DataPointsRowKey rowKey,
                final OperationResult<ColumnList<Integer>> result) {
            final List<DataPoint> datapoints = new ArrayList<DataPoint>();

            for (final Column<Integer> column : result.getResult()) {
                datapoints.add(DataPoint.fromColumn(rowKey, column));
            }

            return datapoints;
        }
    }

    private Callback<DataPointsResult> buildQuery(
            final DataPointsRowKey rowKey, long start, long end)
            throws QueryException {
        final long timestamp = rowKey.getTimestamp();
        final long startTime = DataPoint.Name
                .toStartTimeStamp(start, timestamp);
        final long endTime = DataPoint.Name.toEndTimeStamp(end, timestamp);

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

        final Callback<DataPointsResult> handle = new ConcurrentCallback<DataPointsResult>();

        executor.execute(new QueryCallbackRunnable(handle, queryTimer,
                dataQuery, rowKey));

        return handle;
    }

    private final class FindRowsCallbackRunnable extends
            CallbackRunnable<FindRowsResult> {
        private final FindRows query;
        private final Map<String, String> filter;
        private final RowQuery<String, DataPointsRowKey> dbQuery;

        private FindRowsCallbackRunnable(Callback<FindRowsResult> callback,
                Timer timer, FindRows query, Map<String, String> filter,
                RowQuery<String, DataPointsRowKey> dbQuery) {
            super(callback, timer);
            this.query = query;
            this.filter = filter;
            this.dbQuery = dbQuery;
        }

        @Override
        public FindRowsResult execute() throws Exception {
            final Date then = new Date();
            log.info("{} : {}", backendTags, query);

            final OperationResult<ColumnList<DataPointsRowKey>> result = dbQuery
                    .execute();

            final Date now = new Date();
            final long diff = now.getTime() - then.getTime();
            log.info("{} : {} (took {}ms)", backendTags, query, diff);

            final List<DataPointsRowKey> rowKeys = new ArrayList<DataPointsRowKey>();

            final ColumnList<DataPointsRowKey> columns = result.getResult();

            for (final Column<DataPointsRowKey> column : columns) {
                final DataPointsRowKey rowKey = column.getName();

                if (!matchingTags(rowKey.getTags(), backendTags, filter)) {
                    continue;
                }

                rowKeys.add(rowKey);
            }

            return new FindRowsResult(rowKeys, KairosDBBackend.this);
        }
    }

    @Override
    public Callback<FindRowsResult> findRows(final FindRows query)
            throws QueryException {
        final String key = query.getKey();
        final DateRange range = query.getRange();
        final Map<String, String> filter = query.getFilter();

        final DataPointsRowKey startKey = rowKeyStart(range.start(), key);
        final DataPointsRowKey endKey = rowKeyEnd(range.end(), key);

        final RowQuery<String, DataPointsRowKey> dbQuery = keyspace
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

        final Callback<FindRowsResult> handle = new ConcurrentCallback<FindRowsResult>();

        executor.execute(new FindRowsCallbackRunnable(handle, findRowsTimer,
                query, filter, dbQuery));

        return handle;
    }

    /**
     * The scheduled runnable that executed the given query.
     * 
     * @author udoprog
     */
    private final class FindRowGroupsCallbackRunnable extends
            CallbackRunnable<FindRowGroupsResult> {
        private final RowQuery<String, DataPointsRowKey> dbQuery;
        private final Map<String, String> filter;
        private final List<String> groupBy;

        private FindRowGroupsCallbackRunnable(
                Callback<FindRowGroupsResult> callback, Timer timer,
                RowQuery<String, DataPointsRowKey> dbQuery,
                Map<String, String> filter, List<String> groupBy) {
            super(callback, timer);
            this.dbQuery = dbQuery;
            this.filter = filter;
            this.groupBy = groupBy;
        }

        @Override
        public FindRowGroupsResult execute() throws Exception {
            final OperationResult<ColumnList<DataPointsRowKey>> result = dbQuery
                    .execute();

            final Map<Map<String, String>, List<DataPointsRowKey>> rowGroups = new HashMap<Map<String, String>, List<DataPointsRowKey>>();

            final ColumnList<DataPointsRowKey> columns = result.getResult();

            for (final Column<DataPointsRowKey> column : columns) {
                final DataPointsRowKey rowKey = column.getName();
                final Map<String, String> tags = rowKey.getTags();

                if (!matchingTags(tags, backendTags, filter)) {
                    continue;
                }

                final Map<String, String> key = new HashMap<String, String>();

                for (final String group : groupBy) {
                    key.put(group, tags.get(group));
                }

                List<DataPointsRowKey> rows = rowGroups.get(key);

                if (rows == null) {
                    rows = new ArrayList<DataPointsRowKey>();
                    rowGroups.put(key, rows);
                }

                rows.add(rowKey);
            }

            return new FindRowGroupsResult(rowGroups, KairosDBBackend.this);
        }
    }

    @Override
    public Callback<FindRowGroupsResult> findRowGroups(final FindRowGroups query)
            throws QueryException {
        final String key = query.getKey();
        final DateRange range = query.getRange();
        final Map<String, String> filter = query.getFilter();
        final List<String> groupBy = query.getGroupBy();

        final DataPointsRowKey startKey = rowKeyStart(range.start(), key);
        final DataPointsRowKey endKey = rowKeyEnd(range.end(), key);

        final RowQuery<String, DataPointsRowKey> dbQuery = keyspace
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

        final Callback<FindRowGroupsResult> handle = new ConcurrentCallback<FindRowGroupsResult>();

        executor.execute(new FindRowGroupsCallbackRunnable(handle,
                findRowGroupsTimer, dbQuery, filter, groupBy));

        return handle;
    }

    private final class GetAllRowsCallbackRunnable extends
            CallbackRunnable<GetAllRowsResult> {
        private final AllRowsQuery<String, DataPointsRowKey> rowQuery;

        private GetAllRowsCallbackRunnable(Callback<GetAllRowsResult> callback,
                AllRowsQuery<String, DataPointsRowKey> rowQuery, Timer timer) {
            super(callback, timer);
            this.rowQuery = rowQuery;
        }

        @Override
        public GetAllRowsResult execute() throws Exception {
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
    }

    @Override
    public Callback<GetAllRowsResult> getAllRows() {
        final Callback<GetAllRowsResult> callback = new ConcurrentCallback<GetAllRowsResult>();

        final AllRowsQuery<String, DataPointsRowKey> rowQuery = keyspace
                .prepareQuery(rowKeyIndex).getAllRows();

        executor.execute(new GetAllRowsCallbackRunnable(callback, rowQuery,
                getAllRowsTimer));

        callback.register(new Callback.Ended() {
            @Override
            public void ended() throws Exception {
            }
        });

        return callback;
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

    private DataPointsRowKey rowKeyStart(Date start, String key) {
        final long timeBucket = DataPointsRowKey.getTimeBucket(start);
        return new DataPointsRowKey(key, timeBucket);
    }

    private DataPointsRowKey rowKeyEnd(Date end, String key) {
        final long timeBucket = DataPointsRowKey.getTimeBucket(end);
        return new DataPointsRowKey(key, timeBucket + 1);
    }
}
