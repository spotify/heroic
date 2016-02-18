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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
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
import com.spotify.heroic.metric.Event;
import com.spotify.heroic.metric.FetchData;
import com.spotify.heroic.metric.FetchQuotaWatcher;
import com.spotify.heroic.metric.Metric;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.QueryError;
import com.spotify.heroic.metric.QueryTrace;
import com.spotify.heroic.metric.WriteMetric;
import com.spotify.heroic.metric.bigtable.api.BigtableDataClient;
import com.spotify.heroic.metric.bigtable.api.BigtableTableAdminClient;
import com.spotify.heroic.metric.bigtable.api.ColumnFamily;
import com.spotify.heroic.metric.bigtable.api.Family;
import com.spotify.heroic.metric.bigtable.api.Mutations;
import com.spotify.heroic.metric.bigtable.api.ReadRowsRequest;
import com.spotify.heroic.metric.bigtable.api.Row;
import com.spotify.heroic.metric.bigtable.api.RowFilter;
import com.spotify.heroic.metric.bigtable.api.Table;
import com.spotify.heroic.metrics.Meter;
import com.spotify.heroic.statistics.MetricBackendReporter;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Managed;
import eu.toolchain.async.RetryPolicy;
import eu.toolchain.async.RetryResult;
import eu.toolchain.serializer.BytesSerialWriter;
import eu.toolchain.serializer.Serializer;
import eu.toolchain.serializer.SerializerFramework;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

@BigtableScope
@ToString(of = {"connection"})
@Slf4j
public class BigtableBackend extends AbstractMetricBackend implements LifeCycles {
    /* maxmimum number of cells supported for each batch mutation */
    public static final int MAX_BATCH_SIZE = 10000;

    public static final QueryTrace.Identifier FETCH_SEGMENT =
        QueryTrace.identifier(BigtableBackend.class, "fetch_segment");
    public static final QueryTrace.Identifier FETCH =
        QueryTrace.identifier(BigtableBackend.class, "fetch");

    public static final String POINTS = "points";
    public static final String EVENTS = "events";
    public static final long PERIOD = 0x100000000L;

    private final AsyncFramework async;
    private final SerializerFramework serializer;
    private final Serializer<RowKey> rowKeySerializer;
    private final Managed<BigtableConnection> connection;
    private final Groups groups;
    private final String table;
    private final boolean configure;
    private final MetricBackendReporter reporter;
    private final ObjectMapper mapper;

    private static final TypeReference<Map<String, String>> PAYLOAD_TYPE =
        new TypeReference<Map<String, String>>() {
        };

    private final Meter written = new Meter();
    private final Meter errors = new Meter();

    @Inject
    public BigtableBackend(
        final AsyncFramework async, @Named("common") final SerializerFramework serializer,
        final Serializer<RowKey> rowKeySerializer, final Managed<BigtableConnection> connection,
        final Groups groups, @Named("table") final String table,
        @Named("configure") final boolean configure, MetricBackendReporter reporter,
        @Named("application/json") ObjectMapper mapper
    ) {
        super(async);
        this.async = async;
        this.serializer = serializer;
        this.rowKeySerializer = rowKeySerializer;
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
        registry.watch(this::watch);
    }

    @Override
    public AsyncFuture<Void> configure() {
        return connection.doto(c -> configureMetricsTable(c.tableAdminClient()));
    }

    private AsyncFuture<Void> configureMetricsTable(final BigtableTableAdminClient admin)
        throws IOException {
        return async
            .call(() -> admin.getTable(table))
            .lazyTransform(tableCheck -> tableCheck.map(async::resolved).orElseGet(() -> {
                log.info("Creating missing table: " + table);
                final Table createdTable = admin.createTable(table);

                // check until table exists
                return async
                    .retryUntilResolved(() -> {
                        if (!admin.getTable(createdTable.getName()).isPresent()) {
                            throw new IllegalStateException(
                                "Table does not exist: " + createdTable.toURI());
                        }

                        return async.resolved(createdTable);
                    }, RetryPolicy.timed(10000, RetryPolicy.linear(1000)))
                    .directTransform(RetryResult::getResult);
            }))
            .lazyTransform(metrics -> {
                final List<AsyncFuture<ColumnFamily>> families = new ArrayList<>();

                families.add(async.call(() -> metrics.getColumnFamily(POINTS).orElseGet(() -> {
                    log.info("Creating missing column family: " + POINTS);
                    return admin.createColumnFamily(metrics, POINTS);
                })));

                families.add(async.call(() -> metrics.getColumnFamily(EVENTS).orElseGet(() -> {
                    log.info("Creating missing column family: " + EVENTS);
                    return admin.createColumnFamily(metrics, EVENTS);
                })));

                return async.collectAndDiscard(families);
            });
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
        return connection.doto(c -> {
            final Series series = request.getSeries();
            final List<AsyncFuture<WriteMetric>> results = new ArrayList<>();

            final BigtableDataClient client = c.dataClient();

            final MetricCollection g = request.getData();
            results.add(writeTyped(series, client, g));
            return async.collect(results, WriteMetric.reduce());
        });
    }

    @Override
    public AsyncFuture<FetchData> fetch(
        final FetchData.Request request, final FetchQuotaWatcher watcher
    ) {
        return connection.doto(c -> {
            final MetricType type = request.getType();

            final List<PreparedQuery> prepared =
                ranges(request.getSeries(), request.getRange(), rowKeySerializer);

            if (!watcher.mayReadData()) {
                throw new IllegalArgumentException("query violated data limit");
            }

            switch (type) {
                case POINT:
                    return fetchBatch(watcher, type, POINTS, prepared, c, (t, d) -> {
                        final double value = deserializeValue(d);
                        return new Point(t, value);
                    });
                case EVENT:
                    return fetchBatch(watcher, type, EVENTS, prepared, c, (t, d) -> {
                        final Map<String, String> payload;

                        try {
                            payload = mapper.readValue(d.toByteArray(), PAYLOAD_TYPE);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }

                        return new Event(t, payload);
                    });
                default:
                    return async.resolved(FetchData.error(QueryTrace.of(FETCH),
                        QueryError.fromMessage("unsupported source: " + request.getType())));
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
        final double errorRate = this.errors.getFiveMinuteRate();
        final long errorRatio = (long) (writeRate == 0D ? 0 : errorRate / writeRate) * 100;
        return Statistics.of("written", written, "writeRate", (long) writeRate, "errorRate",
            (long) errorRate, "errorRatio", errorRatio);
    }

    private boolean watch() {
        return errors.getFiveMinuteRate() < 100;
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
        final Series series, final BigtableDataClient client, final MetricCollection g
    ) throws IOException {
        switch (g.getType()) {
            case POINT:
                return writeBatch(POINTS, series, client, g.getDataAs(Point.class),
                    d -> serializeValue(d.getValue()));
            case EVENT:
                return writeBatch(EVENTS, series, client, g.getDataAs(Event.class),
                    BigtableBackend.this::serializeEvent);
            default:
                return async.resolved(WriteMetric.error(
                    QueryError.fromMessage("Unsupported metric type: " + g.getType())));
        }
    }

    private <T extends Metric> AsyncFuture<WriteMetric> writeBatch(
        final String columnFamily, final Series series, final BigtableDataClient client,
        final List<T> batch, final Function<T, ByteString> serializer
    ) throws IOException {
        // common case for consumers
        if (batch.size() == 1) {
            return writeOne(columnFamily, series, client, batch.get(0), serializer).onFinished(
                written::mark);
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

        final ImmutableList.Builder<AsyncFuture<WriteMetric>> writes = ImmutableList.builder();

        final RequestTimer<WriteMetric> timer = WriteMetric.timer();

        for (final Pair<RowKey, Mutations> e : saved) {
            final ByteString rowKeyBytes = serialize(e.getKey(), rowKeySerializer);
            writes.add(client
                .mutateRow(table, rowKeyBytes, e.getValue())
                .directTransform(result -> timer.end()));
        }

        for (final Map.Entry<RowKey, Mutations.Builder> e : building.entrySet()) {
            final ByteString rowKeyBytes = serialize(e.getKey(), rowKeySerializer);
            writes.add(client
                .mutateRow(table, rowKeyBytes, e.getValue().build())
                .directTransform(result -> timer.end()));
        }

        return async
            .collect(writes.build(), WriteMetric.reduce())
            .onFinished(() -> written.mark(batch.size()))
            .onFailed(e -> errors.mark(batch.size()));
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

        final ByteString rowKeyBytes = serialize(rowKey, rowKeySerializer);
        return client
            .mutateRow(table, rowKeyBytes, builder.build())
            .directTransform(result -> timer.end());
    }

    private <T extends Metric> AsyncFuture<FetchData> fetchBatch(
        final FetchQuotaWatcher watcher, final MetricType type, final String columnFamily,
        final List<PreparedQuery> prepared, final BigtableConnection c,
        final BiFunction<Long, ByteString, T> deserializer
    ) {
        final BigtableDataClient client = c.dataClient();

        final List<AsyncFuture<FetchData>> fetches = new ArrayList<>(prepared.size());

        for (final PreparedQuery p : prepared) {
            final AsyncFuture<List<Row>> readRows = client.readRows(table, ReadRowsRequest
                .builder()
                .rowKey(p.keyBlob)
                .filter(RowFilter
                    .newColumnRangeBuilder(columnFamily)
                    .startQualifierOpen(p.startKey)
                    .endQualifierClosed(p.endKey)
                    .build())
                .build());

            final Function<Family.LatestCellValueColumn, T> transform = cell -> {
                final long timestamp = p.base + deserializeOffset(cell.getQualifier());
                return deserializer.apply(timestamp, cell.getValue());
            };

            final QueryTrace.NamedWatch w = QueryTrace.watch(FETCH_SEGMENT);

            fetches.add(readRows.directTransform(result -> {
                watcher.readData(result.size());

                final List<Iterable<T>> points = new ArrayList<>();

                for (final Row row : result) {
                    row.getFamily(columnFamily).ifPresent(f -> {
                        points.add(Iterables.transform(f.latestCellValue(), transform));
                    });
                }

                final QueryTrace trace = w.end();
                final ImmutableList<Long> times = ImmutableList.of(trace.getElapsed());
                final List<T> data =
                    ImmutableList.copyOf(Iterables.mergeSorted(points, Metric.comparator()));
                final List<MetricCollection> groups =
                    ImmutableList.of(MetricCollection.build(type, data));

                return FetchData.of(trace, times, groups);
            }));
        }

        return async.collect(fetches, FetchData.collect(FETCH));
    }

    <T> ByteString serialize(T rowKey, Serializer<T> serializer) throws IOException {
        try (final BytesSerialWriter writer = this.serializer.writeBytes()) {
            serializer.serialize(writer, rowKey);
            return ByteString.copyFrom(writer.toByteArray());
        }
    }

    static long base(long timestamp) {
        return timestamp - timestamp % PERIOD;
    }

    static long offset(long timestamp) {
        return timestamp % PERIOD;
    }

    List<PreparedQuery> ranges(
        final Series series, final DateRange range, final Serializer<RowKey> rowKeySerializer
    ) throws IOException {
        final List<PreparedQuery> bases = new ArrayList<>();

        final long start = base(range.getStart());
        final long end = base(range.getEnd());

        for (long base = start; base <= end; base += PERIOD) {
            final DateRange modified = range.modify(base, base + PERIOD);

            if (modified.isEmpty()) {
                continue;
            }

            final RowKey key = new RowKey(series, base);
            final ByteString keyBlob = serialize(key, rowKeySerializer);
            final ByteString startKey = serializeOffset(offset(modified.start()));
            final ByteString endKey = serializeOffset(offset(modified.end()));

            bases.add(new PreparedQuery(keyBlob, startKey, endKey, base));
        }

        return bases;
    }

    ByteString serializeValue(double value) {
        final ByteBuffer buffer =
            ByteBuffer.allocate(Double.BYTES).putLong(Double.doubleToLongBits(value));
        return ByteString.copyFrom(buffer.array());
    }

    ByteString serializeEvent(Event event) {
        try {
            return ByteString.copyFrom(mapper.writeValueAsBytes(event.getPayload()));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    static double deserializeValue(ByteString value) {
        return Double.longBitsToDouble(ByteBuffer.wrap(value.toByteArray()).getLong());
    }

    /**
     * Offset serialization is sensitive to byte ordering.
     * <p>
     * We require that for two timestamps a, and b, the following invariants hold true.
     * <p>
     * <pre>
     * offset(a) < offset(b)
     * serialized(offset(a)) < serialized(offset(b))
     * </pre>
     * <p>
     * Note: the serialized comparison is performed byte-by-byte, from lowest to highest address.
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

    @RequiredArgsConstructor
    private static final class PreparedQuery {
        private final ByteString keyBlob;
        private final ByteString startKey;
        private final ByteString endKey;
        private final long base;
    }
}
