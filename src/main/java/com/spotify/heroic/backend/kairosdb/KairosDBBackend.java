package com.spotify.heroic.backend.kairosdb;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import lombok.Getter;
import lombok.Setter;
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
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.AllRowsQuery;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.netflix.astyanax.util.RangeBuilder;
import com.spotify.heroic.backend.Backend;
import com.spotify.heroic.backend.MetricBackend;
import com.spotify.heroic.backend.Query;
import com.spotify.heroic.backend.QueryException;
import com.spotify.heroic.query.DateRange;
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
    public List<Query<DataPointsResult>> query(List<DataPointsRowKey> rows,
            DateRange range) throws QueryException {
        final long start = range.start().getTime();
        final long end = range.end().getTime();

        final List<Query<DataPointsResult>> queries = new ArrayList<Query<DataPointsResult>>();

        for (final DataPointsRowKey rowKey : rows) {
            queries.add(buildQuery(rowKey, start, end));
        }

        return queries;
    }

    private Query<DataPointsResult> buildQuery(final DataPointsRowKey rowKey,
            long start, long end) throws QueryException {
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

        final ListenableFuture<OperationResult<ColumnList<Integer>>> listenable;

        try {
            listenable = dataQuery.executeAsync();
        } catch (ConnectionException e) {
            throw new QueryException("Failed to setup query for: " + rowKey, e);
        }

        final Query<DataPointsResult> handle = new Query<DataPointsResult>();

        handle.cancelled(new Query.Cancelled() {
            @Override
            public void cancel() throws Exception {
                listenable.cancel(true);
            }
        });

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
    public Query<FindRowsResult> findRows(String key, DateRange range,
            final Map<String, String> filter) throws QueryException {
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

        final ListenableFuture<OperationResult<ColumnList<DataPointsRowKey>>> listenable;

        try {
            listenable = rowQuery.executeAsync();
        } catch (ConnectionException e) {
            throw new QueryException("Failed to execute request", e);
        }

        final Query<FindRowsResult> handle = new Query<FindRowsResult>();

        listenable.addListener(new Runnable() {
            @Override
            public void run() {
                final OperationResult<ColumnList<DataPointsRowKey>> result;

                try {
                    result = listenable.get();
                } catch (Exception e) {
                    handle.fail(e);
                    return;
                }

                final List<DataPointsRowKey> rowKeys = new ArrayList<DataPointsRowKey>();

                final ColumnList<DataPointsRowKey> columns = result.getResult();

                for (Column<DataPointsRowKey> column : columns) {
                    final DataPointsRowKey rowKey = column.getName();

                    if (!matchingTags(rowKey.getTags(), attributes, filter)) {
                        continue;
                    }

                    rowKeys.add(rowKey);
                }

                handle.finish(new FindRowsResult(rowKeys, KairosDBBackend.this));
            }
        }, executor);

        handle.cancelled(new Query.Cancelled() {
            @Override
            public void cancel() {
                if (!listenable.isCancelled() && !listenable.isDone())
                    listenable.cancel(true);
            }
        });

        return handle;
    }

    @Override
    public Query<FindTagsResult> findTags(final Map<String, String> filter,
            final Set<String> namesFilter) throws QueryException {
        final Query<FindTagsResult> query = new Query<FindTagsResult>();

        final AllRowsQuery<String, DataPointsRowKey> rowQuery = keyspace
                .prepareQuery(rowKeyIndex).getAllRows();

        executor.execute(new Runnable() {
            @Override
            public void run() {
                OperationResult<Rows<String, DataPointsRowKey>> result;

                try {
                    result = rowQuery.execute();
                } catch (ConnectionException e) {
                    query.fail(e);
                    return;
                }

                final Rows<String, DataPointsRowKey> rows = result.getResult();

                final Map<String, Set<String>> tags = new HashMap<String, Set<String>>();
                final List<String> metrics = new ArrayList<String>();

                for (Row<String, DataPointsRowKey> row : rows) {
                    boolean anyMatch = false;

                    for (Column<DataPointsRowKey> column : row.getColumns()) {
                        final DataPointsRowKey rowKey = column.getName();

                        if (!matchingTags(rowKey.getTags(), attributes, filter)) {
                            continue;
                        }

                        for (Map.Entry<String, String> entry : rowKey.getTags()
                                .entrySet()) {
                            if (namesFilter != null
                                    && !namesFilter.contains(entry.getKey())) {
                                continue;
                            }

                            anyMatch = true;

                            Set<String> values = tags.get(entry.getKey());

                            if (values == null) {
                                values = new HashSet<String>();
                                tags.put(entry.getKey(), values);
                            }

                            values.add(entry.getValue());
                        }
                    }

                    if (anyMatch)
                        metrics.add(row.getKey());
                }

                query.finish(new FindTagsResult(tags, metrics));
            }
        });

        return query;
    }

    private static boolean matchingTags(Map<String, String> tags,
            Map<String, String> backendTags, Map<String, String> queryTags) {
        // query not specified.
        if (queryTags != null) {
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
