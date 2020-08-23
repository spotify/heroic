/*
 * Copyright (c) 2015 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.heroic.metric.bigtable;

import static io.opencensus.trace.AttributeValue.booleanAttributeValue;
import static io.opencensus.trace.AttributeValue.longAttributeValue;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigtable.grpc.scanner.FlatRow;
import com.google.cloud.bigtable.util.RowKeyUtil;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.RequestTimer;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.lifecycle.LifeCycleRegistry;
import com.spotify.heroic.lifecycle.LifeCycles;
import com.spotify.heroic.metric.AbstractMetricBackend;
import com.spotify.heroic.metric.BackendEntry;
import com.spotify.heroic.metric.FetchData;
import com.spotify.heroic.metric.FetchQuotaWatcher;
import com.spotify.heroic.metric.Metric;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricReadResult;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.QueryError;
import com.spotify.heroic.metric.QueryTrace;
import com.spotify.heroic.metric.WriteMetric;
import com.spotify.heroic.metric.bigtable.api.BigtableDataClient;
import com.spotify.heroic.metric.bigtable.api.BigtableTableAdminClient;
import com.spotify.heroic.metric.bigtable.api.ColumnFamily;
import com.spotify.heroic.metric.bigtable.api.Mutations;
import com.spotify.heroic.metric.bigtable.api.ReadRowsRequest;
import com.spotify.heroic.metric.bigtable.api.RowFilter;
import com.spotify.heroic.metric.bigtable.api.RowRange;
import com.spotify.heroic.metric.bigtable.api.Table;
import com.spotify.heroic.metrics.Meter;
import com.spotify.heroic.statistics.MetricBackendReporter;
import com.spotify.heroic.tracing.EndSpanFutureReporter;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Managed;
import eu.toolchain.async.RetryPolicy;
import eu.toolchain.async.RetryResult;
import eu.toolchain.serializer.Serializer;
import eu.toolchain.serializer.SerializerFramework;
import io.opencensus.common.Scope;
import io.opencensus.trace.AttributeValue;
import io.opencensus.trace.Span;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.inject.Inject;
import javax.inject.Named;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@BigtableScope
public class BigtableBackend extends AbstractMetricBackend implements LifeCycles {
    private static final Logger log = LoggerFactory.getLogger(BigtableBackend.class);

    /* maximum number of bytes of BigTable row key size allowed*/
    public static final int MAX_KEY_ROW_SIZE = 4000;
    /* maximum number of cells supported for each batch mutation */
    public static final int MAX_BATCH_SIZE = 10000;

    public static final QueryTrace.Identifier FETCH_SEGMENT =
        QueryTrace.identifier(BigtableBackend.class, "fetch_segment");
    public static final QueryTrace.Identifier FETCH =
        QueryTrace.identifier(BigtableBackend.class, "fetch");

    public static final String POINTS = "points";
    public static final String EVENTS = "events";
    public static final long PERIOD = 0x100_000_000L;

    private final AsyncFramework async;
    private final SerializerFramework serializer;
    private final RowKeySerializer rowKeySerializer;
    private final Serializer<SortedMap<String, String>> sortedMapSerializer;
    private final Managed<BigtableConnection> connection;
    private final Groups groups;
    private final String table;
    private final boolean configure;
    private final MetricBackendReporter reporter;
    private final ObjectMapper mapper;
    private final Tracer tracer = Tracing.getTracer();

    private static final TypeReference<Map<String, String>> PAYLOAD_TYPE =
        new TypeReference<Map<String, String>>() {
        };

    private final Meter written = new Meter();

    @Inject
    public BigtableBackend(
        final AsyncFramework async,
        @Named("common") final SerializerFramework serializer,
        final RowKeySerializer rowKeySerializer,
        final Managed<BigtableConnection> connection,
        final Groups groups,
        @Named("table") final String table,
        @Named("configure") final boolean configure,
        MetricBackendReporter reporter,
        @Named("application/json") ObjectMapper mapper
    ) {
        super(async);
        this.async = async;
        this.serializer = serializer;
        this.rowKeySerializer = rowKeySerializer;
        this.sortedMapSerializer = serializer.sortedMap(serializer.string(), serializer.string());
        this.connection = connection;
        this.groups = groups;
        this.table = table;
        this.configure = configure;
        this.reporter = reporter;
        this.mapper = mapper;
    }

    @Override
    public void register(LifeCycleRegistry registry) {
        registry.start(this::start);
        registry.stop(this::stop);
    }

    @Override
    public AsyncFuture<Void> configure() {
        return connection.doto(c -> configureMetricsTable(c.getTableAdminClient()));
    }

    private AsyncFuture<Void> configureMetricsTable(final BigtableTableAdminClient admin) {
        return async.call(() -> {
            final Table createdTable =
                admin.getTable(this.table).orElseGet(() -> admin.createTable(this.table));

            // Wait until table exists.
            final Table table = waitUntilTable(admin, createdTable).get();

            table.getColumnFamily(POINTS).orElseGet(() -> {
                log.info("Creating missing column family: " + POINTS);
                return admin.createColumnFamily(table, POINTS);
            });

            waitUntilColumnFamily(admin, table, POINTS).get();

            table.getColumnFamily(EVENTS).orElseGet(() -> {
                log.info("Creating missing column family: " + EVENTS);
                return admin.createColumnFamily(table, EVENTS);
            });

            waitUntilColumnFamily(admin, table, EVENTS).get();

            return null;
        });
    }

    private AsyncFuture<Table> waitUntilTable(
        final BigtableTableAdminClient admin, final Table table
    ) {
        return waitUntil(() -> admin
            .getTable(table.getName())
            .orElseThrow(
                () -> new IllegalStateException("Table does not exist: " + table.toURI())));
    }

    private AsyncFuture<ColumnFamily> waitUntilColumnFamily(
        final BigtableTableAdminClient admin, final Table originalTable, final String columnFamily
    ) {
        return waitUntil(() -> {
            final Table table = admin
                .getTable(originalTable.getName())
                .orElseThrow(() -> new IllegalStateException(
                    "Table does not exist: " + originalTable.toURI()));

            return table
                .getColumnFamily(columnFamily)
                .orElseThrow(() -> new IllegalStateException(
                    "Column Family does not exist: " + columnFamily));
        });
    }

    private <T> AsyncFuture<T> waitUntil(final Supplier<T> test) {
        return async
            .retryUntilResolved(() -> async.resolved(test.get()),
                RetryPolicy.timed(10000, RetryPolicy.linear(1000)))
            .directTransform(RetryResult::getResult);
    }

    @Override
    public boolean isReady() {
        return connection.isReady();
    }

    @Override
    public Groups groups() {
        return groups;
    }

    @Override
    public AsyncFuture<WriteMetric> write(final WriteMetric.Request request) {
        return write(request, tracer.getCurrentSpan());
    }

    @Override
    public AsyncFuture<WriteMetric> write(
        final WriteMetric.Request request, final Span parentSpan
    ) {
        return connection.doto(c -> {
            final Series series = request.getSeries();
            final List<AsyncFuture<WriteMetric>> results = new ArrayList<>();

            final BigtableDataClient client = c.getDataClient();

            final MetricCollection g = request.getData();
            results.add(writeTyped(series, client, g, parentSpan));
            return async.collect(results, WriteMetric.reduce());
        });
    }

    private List<PreparedQuery> pointsRanges(final FetchData.Request request) throws IOException {
        return ranges(request.getSeries(), request.getRange(), POINTS, (t, d) -> {
            final double value = deserializeValue(d);
            return new Point(t, value);
        });
    }

    @Override
    public AsyncFuture<FetchData.Result> fetch(
        final FetchData.Request request,
        final FetchQuotaWatcher watcher,
        final Consumer<MetricReadResult> consumer,
        final Span parentSpan
    ) {
        return connection.doto(c -> {
            final MetricType type = request.getType();

            if (!watcher.mayReadData()) {
                throw new IllegalArgumentException("query violated data limit");
            }

            switch (type) {
                case POINT:
                    return fetchBatch(
                        watcher, type, pointsRanges(request), c, consumer, parentSpan);
                default:
                    return async.resolved(new FetchData.Result(QueryTrace.of(FETCH),
                        new QueryError("unsupported source: " + request.getType())));
            }
        });
    }

    @Override
    public Iterable<BackendEntry> listEntries() {
        return ImmutableList.of();
    }

    @Override
    public Statistics getStatistics() {
        final long written = this.written.getCount();
        final double writeRate = this.written.getFiveMinuteRate();
        return new Statistics("written", written, "writeRate", (long) writeRate);
    }

    private AsyncFuture<Void> start() {
        final AsyncFuture<Void> future = connection.start();

        if (!configure) {
            return future;
        }

        return future.lazyTransform(v -> configure());
    }

    private AsyncFuture<Void> stop() {
        return connection.stop();
    }

    private AsyncFuture<WriteMetric> writeTyped(
        final Series series,
        final BigtableDataClient client,
        final MetricCollection g,
        final Span parentSpan
    ) throws IOException {
        switch (g.getType()) {
            case POINT:
                return writeBatch(POINTS, series, client, g.getDataAs(Point.class),
                    d -> serializeValue(d.getValue()), parentSpan);
            default:
                return async.resolved(new WriteMetric(
                    new QueryError("Unsupported metric type: " + g.getType())));
        }
    }

    private <T extends Metric> AsyncFuture<WriteMetric> writeBatch(
        final String columnFamily, final Series series, final BigtableDataClient client,
        final List<T> batch, final Function<T, ByteString> serializer, final Span parentSpan
    ) throws IOException {
        final Span span = tracer
            .spanBuilderWithExplicitParent("Bigtable.writeBatch", parentSpan)
            .startSpan();
        final Scope scope = tracer.withSpan(span);
        span.putAttribute("type", AttributeValue.stringAttributeValue(columnFamily));
        span.putAttribute("batchSize", longAttributeValue(batch.size()));

        // common case for consumers
        if (batch.size() == 1) {
            final AsyncFuture<WriteMetric> future =
                writeOne(columnFamily, series, client, batch.get(0), serializer)
                    .onFinished(() -> {
                        written.mark();
                        span.end();
                    });
            scope.close();
            return future;
        }

        final List<Pair<RowKey, Mutations>> saved = new ArrayList<>();
        final Map<RowKey, Mutations.Builder> building = new HashMap<>();

        for (final T d : batch) {
            final long timestamp = d.getTimestamp();
            final long base = base(timestamp);
            final long offset = offset(timestamp);

            final RowKey rowKey = new RowKey(series, base);

            Mutations.Builder builder = building.get(rowKey);

            final ByteString offsetBytes = serializeOffset(offset);
            final ByteString valueBytes = serializer.apply(d);

            if (builder == null) {
                builder = Mutations.builder();
                building.put(rowKey, builder);
            }

            builder.setCell(columnFamily, offsetBytes, valueBytes);

            if (builder.size() >= MAX_BATCH_SIZE) {
                saved.add(Pair.of(rowKey, builder.build()));
                building.put(rowKey, Mutations.builder());
            }
        }

        building
            .entrySet()
            .stream()
            .filter(e -> e.getValue().size() > 0)
            .map(e -> Pair.of(e.getKey(), e.getValue().build()))
            .forEach(saved::add);

        final ImmutableList.Builder<AsyncFuture<WriteMetric>> writes = ImmutableList.builder();

        final RequestTimer<WriteMetric> timer = WriteMetric.timer();

        for (final Pair<RowKey, Mutations> e : saved) {
            final ByteString rowKeyBytes = rowKeySerializer.serializeFull(e.getKey());

            if (rowKeyBytes.size() >= MAX_KEY_ROW_SIZE) {
                reporter.reportWritesDroppedBySize();
                log.error("Row key length greater than 4096 bytes (2): " + rowKeyBytes.size()
                    + " " + rowKeyBytes);
                continue;
            }

            writes.add(client
                .mutateRow(table, rowKeyBytes, e.getValue())
                .directTransform(result -> timer.end()));
        }

        scope.close();
        return async.collect(writes.build(), WriteMetric.reduce()).onFinished(span::end);
    }

    private <T extends Metric> AsyncFuture<WriteMetric> writeOne(
        final String columnFamily, Series series, BigtableDataClient client, T p,
        Function<T, ByteString> serializer
    ) throws IOException {
        final long timestamp = p.getTimestamp();
        final long base = base(timestamp);
        final long offset = offset(timestamp);

        final RowKey rowKey = new RowKey(series, base);

        final Mutations.Builder builder = Mutations.builder();

        final ByteString offsetBytes = serializeOffset(offset);
        final ByteString valueBytes = serializer.apply(p);

        builder.setCell(columnFamily, offsetBytes, valueBytes);

        final RequestTimer<WriteMetric> timer = WriteMetric.timer();

        final ByteString rowKeyBytes = rowKeySerializer.serializeFull(rowKey);

        if (rowKeyBytes.size() >= MAX_KEY_ROW_SIZE) {
            reporter.reportWritesDroppedBySize();
            log.error("Row key length greater than 4096 bytes (1): " +
                rowKeyBytes.size() + " " + rowKey);
            return async.resolved().directTransform(result -> timer.end());
        }

        return client
            .mutateRow(table, rowKeyBytes, builder.build())
            .directTransform(result -> timer.end());
    }

    private AsyncFuture<FetchData.Result> fetchBatch(
        final FetchQuotaWatcher watcher,
        final MetricType type,
        final List<PreparedQuery> prepared,
        final BigtableConnection c,
        final Consumer<MetricReadResult> metricsConsumer,
        final Span parentSpan
    ) {
        final BigtableDataClient client = c.getDataClient();

        final Span fetchBatchSpan =
            tracer.spanBuilderWithExplicitParent("bigtable.fetchBatch", parentSpan).startSpan();
        fetchBatchSpan.putAttribute("preparedQuerySize", longAttributeValue(prepared.size()));

        final List<AsyncFuture<FetchData.Result>> fetches = new ArrayList<>(prepared.size());

        for (final PreparedQuery p : prepared) {
            Span readRowsSpan = tracer.spanBuilderWithExplicitParent(
                "bigtable.readRows", fetchBatchSpan).startSpan();
            readRowsSpan.putAttribute("rowKeyBaseTimestamp", longAttributeValue(p.base));

            final Function<FlatRow.Cell, Metric> transform =
                cell -> p.deserialize(cell.getQualifier(), cell.getValue());

            QueryTrace.NamedWatch fs = QueryTrace.watch(FETCH_SEGMENT);

            final AsyncFuture<List<FlatRow>> readRows;
            try (Scope ignored = tracer.withSpan(fetchBatchSpan)) {
                readRows =
                    client
                        .readRows(
                            table,
                            ReadRowsRequest.builder()
                                .range(new RowRange(
                                    Optional.of(p.rowKeyStart), Optional.of(p.rowKeyEnd)))
                                .filter(
                                    RowFilter.chain(
                                        Arrays.asList(
                                            RowFilter.newColumnRangeBuilder(p.columnFamily)
                                                .startQualifierOpen(p.startQualifierOpen)
                                                .endQualifierClosed(p.endQualifierClosed)
                                                .build(),
                                            RowFilter.onlyLatestCell())))
                                .build())
                        .onDone(new EndSpanFutureReporter(readRowsSpan));
            }

            final AtomicBoolean foundResourceIdentifier = new AtomicBoolean(false);
            fetches.add(readRows.directTransform(result -> {
                readRowsSpan.putAttribute("rowsReturned", longAttributeValue(result.size()));
                for (final FlatRow row : result) {
                    SortedMap<String, String> resource = parseResourceFromRowKey(row.getRowKey());

                    if (!foundResourceIdentifier.get() && resource.size() > 0) {
                        foundResourceIdentifier.set(true);
                    }

                    watcher.readData(row.getCells().size());

                    final List<Metric> metrics = Lists.transform(row.getCells(), transform);
                    final MetricCollection mc = MetricCollection.build(type, metrics);
                    final MetricReadResult readResult = new MetricReadResult(mc, resource);

                    metricsConsumer.accept(readResult);
                }

                readRowsSpan.putAttribute(
                    "containsResourceIdentifier", booleanAttributeValue(
                        foundResourceIdentifier.get()));

                return new FetchData.Result(fs.end());
            }));
        }

        return async.collect(fetches, FetchData
            .collectResult(FETCH))
            .directTransform(result -> {
                fetchBatchSpan.end();
                // this is not actually how many rows were touched. The number of resource
                // identifiers are unknown until query time.
                watcher.accessedRows(prepared.size());
                return result;
            });
    }

    private SortedMap<String, String> parseResourceFromRowKey(final ByteString rowKey)
        throws IOException {
        return rowKeySerializer
            .deserializeFull(ByteBuffer.wrap(rowKey.toByteArray()))
            .getSeries()
            .getResource();
    }

    static long base(long timestamp) {
        return timestamp - timestamp % PERIOD;
    }

    static long offset(long timestamp) {
        return timestamp % PERIOD;
    }

    List<PreparedQuery> ranges(
        final Series series,
        final DateRange range,
        final String columnFamily,
        final BiFunction<Long, ByteString, Metric> deserializer
    ) throws IOException {
        final List<PreparedQuery> bases = new ArrayList<>();

        final long start = base(range.getStart());
        final long end = base(range.getEnd());

        for (long base = start; base <= end; base += PERIOD) {
            final DateRange modified = range.modify(base, base + PERIOD);

            if (modified.isEmpty()) {
                continue;
            }

            final ByteString key = rowKeySerializer.serializeMinimal(
                new RowKeyMinimal(RowKeyMinimal.Series.create(series), base));

            final ByteString keyEnd = ByteString.copyFrom(
                RowKeyUtil.calculateTheClosestNextRowKeyForPrefix(key.toByteArray()));

            final ByteString columnStart = serializeOffset(offset(modified.start()));
            final ByteString columnEnd = serializeOffset(offset(modified.end()));

            bases.add(
                new PreparedQuery(
                    key, keyEnd, columnFamily, columnStart, columnEnd, deserializer, base));
        }

        return bases;
    }

    ByteString serializeValue(double value) {
        final ByteBuffer buffer =
            ByteBuffer.allocate(Double.BYTES).putLong(Double.doubleToLongBits(value));
        return ByteString.copyFrom(buffer.array());
    }

    static double deserializeValue(ByteString value) {
        return Double.longBitsToDouble(ByteBuffer.wrap(value.toByteArray()).getLong());
    }

    /**
     * Offset serialization is sensitive to byte ordering. <p> We require that for two timestamps a,
     * and b, the following invariants hold true. <p>
     * <pre>
     * offset(a) < offset(b)
     * serialized(offset(a)) < serialized(offset(b))
     * </pre>
     * <p> Note: the serialized comparison is performed byte-by-byte, from lowest to highest
     * address.
     *
     * @param offset Offset to serialize
     * @return A byte array, containing the serialized offset.
     */
    static ByteString serializeOffset(long offset) {
        if (offset >= PERIOD) {
            throw new IllegalArgumentException("can only serialize 32-bit wide values");
        }

        final byte[] bytes = new byte[4];
        bytes[0] = (byte) ((offset >> 24) & 0xff);
        bytes[1] = (byte) ((offset >> 16) & 0xff);
        bytes[2] = (byte) ((offset >> 8) & 0xff);
        bytes[3] = (byte) ((offset >> 0) & 0xff);
        return ByteString.copyFrom(bytes);
    }

    static long deserializeOffset(ByteString value) {
        final byte[] bytes = value.toByteArray();

        // @formatter:off
        return ((long) (bytes[0] & 0xff) << 24) +
               ((long) (bytes[1] & 0xff) << 16) +
               ((long) (bytes[2] & 0xff) << 8) +
               ((long) (bytes[3] & 0xff) << 0);
        // @formatter:on
    }

    public String toString() {
        return "BigtableBackend(connection=" + this.connection + ")";
    }

    private static final class PreparedQuery {
        private final ByteString rowKeyStart;
        private final ByteString rowKeyEnd;
        private final String columnFamily;
        private final ByteString startQualifierOpen;
        private final ByteString endQualifierClosed;
        private final BiFunction<Long, ByteString, Metric> deserializer;
        private final long base;

        @java.beans.ConstructorProperties({ "rowKeyStart", "rowKeyEnd", "columnFamily",
                                            "startQualifierOpen", "endQualifierClosed",
                                            "deserializer",
                                            "base" })
        public PreparedQuery(final ByteString rowKeyStart,
                             final ByteString rowKeyEnd,
                             final String columnFamily,
                             final ByteString startQualifierOpen,
                             final ByteString endQualifierClosed,
                             final BiFunction<Long, ByteString, Metric> deserializer,
                             final long base) {
            this.rowKeyStart = rowKeyStart;
            this.rowKeyEnd = rowKeyEnd;
            this.columnFamily = columnFamily;
            this.startQualifierOpen = startQualifierOpen;
            this.endQualifierClosed = endQualifierClosed;
            this.deserializer = deserializer;
            this.base = base;
        }

        private Metric deserialize(final ByteString qualifier, final ByteString value) {
            final long timestamp = base + deserializeOffset(qualifier);
            return deserializer.apply(timestamp, value);
        }
    }
}
