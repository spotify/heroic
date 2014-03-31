package com.spotify.heroic.backend.kairosdb;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.AstyanaxConfiguration;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.netflix.astyanax.util.RangeBuilder;
import com.spotify.heroic.backend.Backend;
import com.spotify.heroic.backend.GroupQuery;
import com.spotify.heroic.backend.MetricBackend;
import com.spotify.heroic.backend.Query;
import com.spotify.heroic.backend.QueryException;
import com.spotify.heroic.query.DateRange;
import com.spotify.heroic.query.MetricsQuery;
import com.spotify.heroic.yaml.Utils;
import com.spotify.heroic.yaml.ValidationException;

@Slf4j
public class KairosDBBackend implements MetricBackend {
    public static class YAML implements Backend.YAML {
        public static final String TYPE = "!kairosdb-backend";

        @Getter
        @Setter
        private String seeds;

        @Getter
        @Setter
        private String keyspace = "kairosdb";

        @Getter
        @Setter
        private Map<String, String> attributes;

        @Override
        public Backend build(String context) throws ValidationException {
            Utils.notEmpty(context + ".keyspace", this.keyspace);
            Utils.notEmpty(context + ".seeds", this.seeds);
            final Map<String, String> attributes = Utils.toMap(context,
                    this.attributes);
            return new KairosDBBackend(keyspace, seeds, attributes);
        }
    }

    @ToString(of = { "attributes", "rowKeys" })
    public static class KairosDBDataPointEngine implements DataPointsQuery {
        private final List<DataPointsRowKey> rowKeys;
        private final Map<String, String> attributes;
        private final Keyspace keyspace;
        private final ColumnFamily<DataPointsRowKey, Integer> dataPoints;
        private final Executor executor;

        public KairosDBDataPointEngine(List<DataPointsRowKey> rowKeys,
                Map<String, String> attributes, Keyspace keyspace,
                ColumnFamily<DataPointsRowKey, Integer> dataPoints,
                Executor executor) {
            this.rowKeys = rowKeys;
            this.attributes = attributes;
            this.keyspace = keyspace;
            this.dataPoints = dataPoints;
            this.executor = executor;
        }

        @Override
        public GroupQuery<DataPointsResult> query(MetricsQuery query)
                throws QueryException {
            final DateRange dateRange = query.getRange();

            final long start = dateRange.start().getTime();
            final long end = dateRange.end().getTime();

            final List<Query<DataPointsResult>> queries = new ArrayList<Query<DataPointsResult>>();

            for (final DataPointsRowKey rowKey : rowKeys) {
                queries.add(buildHandle(rowKey, start, end));
            }

            return new GroupQuery<DataPointsResult>(queries);
        }

        private Query<DataPointsResult> buildHandle(
                final DataPointsRowKey rowKey, long start, long end)
                throws QueryException {
            final long timestamp = rowKey.getTimestamp();
            final long startTime = DataPoint.Name.toStartTimeStamp(start,
                    timestamp);
            final long endTime = DataPoint.Name.toEndTimeStamp(end, timestamp);

            final RowQuery<DataPointsRowKey, Integer> dataQuery = keyspace
                    .prepareQuery(dataPoints)
                    .getRow(rowKey)
                    .autoPaginate(true)
                    .withColumnRange(
                            new RangeBuilder()
                                    .setStart((int) startTime,
                                            IntegerSerializer.get())
                                    .setEnd((int) endTime,
                                            IntegerSerializer.get()).build());

            final ListenableFuture<OperationResult<ColumnList<Integer>>> listenable;

            try {
                listenable = dataQuery.executeAsync();
            } catch (ConnectionException e) {
                throw new QueryException(
                        "Failed to setup query for: " + rowKey, e);
            }

            final Query<DataPointsResult> handle = new Query<DataPointsResult>();

            listenable.addListener(new Runnable() {
                @Override
                public void run() {
                    final OperationResult<ColumnList<Integer>> result;

                    try {
                        result = listenable.get();
                    } catch (Exception e) {
                        handle.fail(e);
                        return;
                    }

                    final List<DataPoint> datapoints;

                    try {
                        datapoints = buildDataPoints(rowKey, result);
                    } catch (Throwable t) {
                        handle.fail(t);
                        return;
                    }

                    handle.finish(new DataPointsResult(datapoints, rowKey));
                }

                private List<DataPoint> buildDataPoints(
                        final DataPointsRowKey rowKey,
                        final OperationResult<ColumnList<Integer>> result) {
                    final List<DataPoint> dp = new ArrayList<DataPoint>();

                    for (final Column<Integer> column : result.getResult()) {
                        dp.add(DataPoint.fromColumn(rowKey, column));
                    }

                    return dp;
                }
            }, executor);

            return handle;
        }

        @Override
        public boolean isEmpty() {
            return rowKeys.isEmpty();
        }
    }

    private final Executor executor = Executors.newFixedThreadPool(20);
    private final Map<String, String> attributes;
    private final Keyspace keyspace;
    private final ColumnFamily<DataPointsRowKey, Integer> dataPoints;
    private final ColumnFamily<String, DataPointsRowKey> rowKeyIndex;

    private static final String CF_DATA_POINTS_NAME = "data_points";
    private static final String CF_ROW_KEY_INDEX = "row_key_index";

    public KairosDBBackend(String keyspace, String seeds,
            Map<String, String> attributes) {

        AstyanaxConfiguration config = new AstyanaxConfigurationImpl()
                .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
                .setConnectionPoolType(ConnectionPoolType.TOKEN_AWARE);

        AstyanaxContext<Keyspace> ctx = new AstyanaxContext.Builder()
                .withConnectionPoolConfiguration(
                        new ConnectionPoolConfigurationImpl(
                                "HeroicConnectionPool").setPort(9160)
                                .setMaxConnsPerHost(1).setSeeds(seeds))
                .forKeyspace(keyspace).withAstyanaxConfiguration(config)
                .buildKeyspace(ThriftFamilyFactory.getInstance());

        ctx.start();

        this.dataPoints = new ColumnFamily<DataPointsRowKey, Integer>(
                CF_DATA_POINTS_NAME, DataPointsRowKey.Serializer.get(),
                IntegerSerializer.get());

        this.rowKeyIndex = new ColumnFamily<>(CF_ROW_KEY_INDEX,
                StringSerializer.get(), DataPointsRowKey.Serializer.get());

        this.keyspace = ctx.getClient();
        this.attributes = attributes;
    }

    @Override
    public Query<DataPointsQuery> query(MetricsQuery query)
            throws QueryException {
        final ListenableFuture<OperationResult<ColumnList<DataPointsRowKey>>> listenable = findDataPointRowKeys(query);

        final Query<DataPointsQuery> handle = new Query<DataPointsQuery>();

        handle.listen(new Query.Handle<DataPointsQuery>() {
            @Override
            public void cancel() {
                // if cancelled, try to cancel the database future.
                if (!listenable.isCancelled() || !listenable.isDone()) {
                    listenable.cancel(true);
                }
            }

            @Override
            public void error(Throwable e) throws Exception {
                // we do not care about errors (here).
            }

            @Override
            public void finish(DataPointsQuery engine) throws Exception {
                // we do not care about finish.
            }
        });

        final Map<String, String> queryAttributes = query.getAttributes();

        listenable.addListener(new Runnable() {
            @Override
            public void run() {
                final OperationResult<ColumnList<DataPointsRowKey>> operationResult;

                try {
                    operationResult = listenable.get();
                } catch (Exception e) {
                    handle.fail(e);
                    return;
                }

                final List<DataPointsRowKey> rowKeys = new ArrayList<DataPointsRowKey>();

                final ColumnList<DataPointsRowKey> columns = operationResult
                        .getResult();

                for (Column<DataPointsRowKey> column : columns) {
                    final DataPointsRowKey rowKey = column.getName();

                    if (!matchingTags(rowKey.getTags(), attributes,
                            queryAttributes)) {
                        continue;
                    }

                    rowKeys.add(rowKey);
                }

                handle.finish(new KairosDBDataPointEngine(rowKeys, attributes,
                        keyspace, dataPoints, executor));
            }
        }, executor);

        return handle;
    }

    private ListenableFuture<OperationResult<ColumnList<DataPointsRowKey>>> findDataPointRowKeys(
            MetricsQuery query) throws QueryException {
        final DateRange dateRange = query.getRange();
        final String key = query.getKey();

        final DataPointsRowKey startKey = rowKeyStart(dateRange.start(), key);
        final DataPointsRowKey endKey = rowKeyEnd(dateRange.end(), key);

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

        try {
            return rowQuery.executeAsync();
        } catch (ConnectionException e) {
            throw new QueryException("Failed to execute request", e);
        }

    }

    private static boolean matchingTags(Map<String, String> tags,
            Map<String, String> backendTags, Map<String, String> queryTags) {
        // query not specified.
        if (queryTags == null || queryTags.isEmpty())
            return true;

        // match the row tags with the query tags.
        for (Map.Entry<String, String> entry : queryTags.entrySet()) {
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

        return true;
    }

    private DataPointsRowKey rowKeyStart(Date start, String key) {
        long timeBucket = DataPointsRowKey.getTimeBucket(start);
        return new DataPointsRowKey(key, timeBucket);
    }

    private DataPointsRowKey rowKeyEnd(Date end, String key) {
        long timeBucket = DataPointsRowKey.getTimeBucket(end);
        return new DataPointsRowKey(key, timeBucket + 1);
    }
}
