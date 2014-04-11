package com.spotify.heroic.backend.kairosdb;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import lombok.Getter;
import lombok.Setter;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
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
import com.spotify.heroic.yaml.Utils;
import com.spotify.heroic.yaml.ValidationException;

public class KairosDBBackend implements MetricBackend {
    public static final String QUERY = "query";
    public static final String GET_COLUMN_COUNT = "get-column-count";
    private static final String GET_ALL_ROWS = "get-all-rows";
    private static final String FIND_ROWS = "find-rows";
    private static final String FIND_ROW_GROUPS = "find-row-groups";

    private final class QueryRunnable extends
            CallbackRunnable<DataPointsResult> {
        private final RowQuery<DataPointsRowKey, Integer> dataQuery;
        private final DataPointsRowKey rowKey;

        private QueryRunnable(Timer timer, Callback<DataPointsResult> callback,
                RowQuery<DataPointsRowKey, Integer> dataQuery,
                DataPointsRowKey rowKey) {
            super(QUERY, timer, callback);
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

    private Session cqlSession;
    private PreparedStatement countStatement;

    private final Timer queryTimer;
    private final Timer findRowsTimer;
    private final Timer findRowGroupsTimer;
    private final Timer getAllRowsTimer;
    private final Timer getColumnCountTimer;

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

        initializeCqlSession(seeds, keyspace);

        this.rowKeyIndex = new ColumnFamily<>(CF_ROW_KEY_INDEX,
                StringSerializer.get(), DataPointsRowKey.Serializer.get());

        this.keyspace = ctx.getClient();

        this.backendTags = backendTags;

        /* setup metric timers */
        this.queryTimer = registry.timer(MetricRegistry.name("heroic",
                "kairosdb", QUERY, backendTags.toString()));
        this.findRowGroupsTimer = registry.timer(MetricRegistry.name("heroic",
                "kairosdb", FIND_ROW_GROUPS, backendTags.toString()));
        this.findRowsTimer = registry.timer(MetricRegistry.name("heroic",
                "kairosdb", FIND_ROWS, backendTags.toString()));
        this.getAllRowsTimer = registry.timer(MetricRegistry.name("heroic",
                "kairosdb", GET_ALL_ROWS, backendTags.toString()));
        this.getColumnCountTimer = registry.timer(MetricRegistry.name("heroic",
                "kairosdb", GET_COLUMN_COUNT, backendTags.toString()));
    }

    @Override
    public List<Callback<DataPointsResult>> query(List<DataPointsRowKey> rows,
            Date start, Date end) {
        final long startTime = start.getTime();
        final long endTime = end.getTime();

        final List<Callback<DataPointsResult>> queries = new ArrayList<Callback<DataPointsResult>>();

        for (final DataPointsRowKey rowKey : rows) {
            final Callback<DataPointsResult> callback = buildQuery(rowKey,
                    startTime, endTime);

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

    private Callback<DataPointsResult> buildQuery(
            final DataPointsRowKey rowKey, long start, long end) {
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

        final Callback<DataPointsResult> callback = new ConcurrentCallback<DataPointsResult>();

        executor.execute(new QueryRunnable(queryTimer, callback, dataQuery,
                rowKey));

        return callback;
    }

    private final class GetColumnCountRunnable extends CallbackRunnable<Long> {
        private final long endTime;
        private final long startTime;
        private final DataPointsRowKey row;

        private GetColumnCountRunnable(Timer timer, Callback<Long> callback,
                long endTime, long startTime, DataPointsRowKey row) {
            super(GET_COLUMN_COUNT, timer, callback);
            this.endTime = endTime;
            this.startTime = startTime;
            this.row = row;
        }

        @Override
        public Long execute() throws Exception {
            final long timestamp = row.getTimestamp();
            final long startColumn = DataPoint.Name.toStartTimeStamp(startTime,
                    timestamp);
            final long endColumn = DataPoint.Name.toEndTimeStamp(endTime,
                    timestamp);

            Long columnCount = null;

            BoundStatement query = new BoundStatement(countStatement);
            query = query.bind(
                    DataPointsRowKey.Serializer.get().toByteBuffer(row),
                    IntegerSerializer.get().toByteBuffer((int) startColumn),
                    IntegerSerializer.get().toByteBuffer((int) endColumn));

            final Iterator<com.datastax.driver.core.Row> iterator = cqlSession
                    .execute(query).iterator();

            if (iterator.hasNext()) {
                final com.datastax.driver.core.Row row = iterator.next();
                columnCount = row.getLong("\"count\"");
            }

            return columnCount;
        }
    }

    @Override
    public Callback<Long> getColumnCount(final DataPointsRowKey row,
            Date start, Date end) {
        final long startTime = start.getTime();
        final long endTime = end.getTime();

        final Callback<Long> callback = new ConcurrentCallback<Long>();

        executor.execute(new GetColumnCountRunnable(getColumnCountTimer,
                callback, endTime, startTime, row));

        return callback;
    }

    private final class FindRowsRunnable extends
            CallbackRunnable<FindRowsResult> {
        private final RowQuery<String, DataPointsRowKey> query;
        private final Map<String, String> filter;

        private FindRowsRunnable(Timer timer,
                Callback<FindRowsResult> callback,
                RowQuery<String, DataPointsRowKey> dbQuery,
                Map<String, String> filter) {
            super(FIND_ROWS, timer, callback);
            this.query = dbQuery;
            this.filter = filter;
        }

        @Override
        public FindRowsResult execute() throws Exception {
            final OperationResult<ColumnList<DataPointsRowKey>> result = query
                    .execute();

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
    public Callback<FindRowsResult> findRows(final FindRows criteria) {
        final String key = criteria.getKey();
        final Map<String, String> filter = criteria.getFilter();

        RowQuery<String, DataPointsRowKey> query = keyspace
                .prepareQuery(rowKeyIndex).getRow(key).autoPaginate(true);

        final Date start = criteria.getStart();
        final Date end = criteria.getEnd();

        // if range specified, filter columns.
        if (start != null && end != null) {
            final DataPointsRowKey startKey = rowKeyStart(start, key);
            final DataPointsRowKey endKey = rowKeyEnd(end, key);
            query = query.withColumnRange(new RangeBuilder()
                    .setStart(startKey, DataPointsRowKey.Serializer.get())
                    .setEnd(endKey, DataPointsRowKey.Serializer.get()).build());
        }

        final Callback<FindRowsResult> callback = new ConcurrentCallback<FindRowsResult>();

        executor.execute(new FindRowsRunnable(findRowsTimer, callback, query,
                filter));

        return callback;
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
            super(FIND_ROW_GROUPS, timer, callback);
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
        final Date start = query.getStart();
        final Date end = query.getEnd();
        final Map<String, String> filter = query.getFilter();
        final List<String> groupBy = query.getGroupBy();

        final DataPointsRowKey startKey = rowKeyStart(start, key);
        final DataPointsRowKey endKey = rowKeyEnd(end, key);

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
            super(GET_ALL_ROWS, timer, callback);
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

    private String[] getHosts(String seed) {
        final List<String> hosts = new ArrayList<String>();
        final String[] hostPorts = seed.split(",");
        for (final String hp : hostPorts) {
            final String[] split = hp.split(":");
            hosts.add(split[0]);
        }
        return hosts.toArray(new String[hosts.size()]);
    }

    private void initializeCqlSession(String seeds, String keyspace) {
        Builder builder = Cluster.builder();
        builder = builder.addContactPoints(getHosts(seeds));
        final Cluster cluster = builder.build();
        cqlSession = cluster.connect(keyspace);
        final String queryStr = "SELECT count(*) FROM " + dataPoints.getName()
                + " WHERE key = ? AND column1 > ? AND column1 < ?";

        countStatement = cqlSession.prepare(queryStr);
    }
}
