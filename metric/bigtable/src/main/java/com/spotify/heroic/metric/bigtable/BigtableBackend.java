package com.spotify.heroic.metric.bigtable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.inject.name.Named;
import com.google.protobuf.ByteString;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.LifeCycle;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.AbstractMetricBackend;
import com.spotify.heroic.metric.BackendEntry;
import com.spotify.heroic.metric.BackendKey;
import com.spotify.heroic.metric.FetchData;
import com.spotify.heroic.metric.FetchQuotaWatcher;
import com.spotify.heroic.metric.Metric;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.MetricTypedGroup;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.WriteMetric;
import com.spotify.heroic.metric.WriteResult;
import com.spotify.heroic.metric.bigtable.api.BigtableCell;
import com.spotify.heroic.metric.bigtable.api.BigtableClient;
import com.spotify.heroic.metric.bigtable.api.BigtableColumnFamily;
import com.spotify.heroic.metric.bigtable.api.BigtableLatestColumnFamily;
import com.spotify.heroic.metric.bigtable.api.BigtableLatestRow;
import com.spotify.heroic.metric.bigtable.api.BigtableMutations;
import com.spotify.heroic.metric.bigtable.api.BigtableTable;
import com.spotify.heroic.metric.bigtable.api.BigtableTableAdminClient;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.FutureDone;
import eu.toolchain.async.Managed;
import eu.toolchain.async.ManagedAction;
import eu.toolchain.async.ResolvableFuture;
import eu.toolchain.async.Transform;
import eu.toolchain.serializer.SerialWriter;
import eu.toolchain.serializer.Serializer;
import eu.toolchain.serializer.SerializerFramework;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@ToString(of = { "connection" })
@Slf4j
public class BigtableBackend extends AbstractMetricBackend implements LifeCycle {
    // XXX: replace w/ Integer.BYTES when upgrading to java 8
    private static final int OFFSET_BYTES = 4;
    // XXX: replace w/ Double.BYTES when upgrading to java 8
    private static final int LONG_BYTES = 8;

    public static final String METRICS = "metrics";
    public static final String POINTS = "points";
    public static final long PERIOD = 0x100000000L;

    private final AsyncFramework async;
    private final SerializerFramework serializer;
    private final Serializer<RowKey> rowKeySerializer;
    private final Managed<BigtableConnection> connection;
    private final Groups groups;

    public BigtableBackend(final AsyncFramework async, final SerializerFramework serializer,
            @Named("common") final Serializer<RowKey> rowKeySerializer, final Managed<BigtableConnection> connection,
            final Groups groups) {
        super(async);
        this.async = async;
        this.serializer = serializer;
        this.rowKeySerializer = rowKeySerializer;
        this.connection = connection;
        this.groups = groups;
    }

    @Override
    public AsyncFuture<Void> start() {
        return connection.start();
    }

    @Override
    public AsyncFuture<Void> stop() {
        return connection.stop();
    }

    @Override
    public AsyncFuture<Void> configure() {
        return connection.doto(new ManagedAction<BigtableConnection, Void>() {
            @Override
            public AsyncFuture<Void> action(final BigtableConnection c) throws Exception {
                return async.call(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        final BigtableTableAdminClient admin = c.adminClient();
                        final Map<String, BigtableTable> tables = toTableNames(admin.listTablesDetails());

                        log.info("Tables: {}", tables);
                        configureMetricsTable(admin, tables);

                        return null;
                    }

                    private void configureMetricsTable(final BigtableTableAdminClient admin,
                            final Map<String, BigtableTable> tables) throws IOException {

                        if (!tables.containsKey(METRICS)) {
                            log.info("Creating non-existant table {}", METRICS);
                            admin.createTable(admin.table(METRICS).columnFamily(admin.columnFamily("points").build())
                                    .build());
                        } else {
                            log.info("Table {} already exists", METRICS);
                        }

                        final BigtableTable metrics = tables.get(METRICS);

                        final Map<String, BigtableColumnFamily> columnFamilies = toColumnFamilyNames(metrics
                                .getColumnFamilies());

                        if (!columnFamilies.containsKey(POINTS)) {
                            log.info("Creating non-existant column family {}:{}", METRICS, POINTS);
                            admin.createColumnFamily(METRICS, admin.columnFamily(POINTS).build());
                        } else {
                            log.info("Column family {}:{} already exists", METRICS, POINTS);
                        }
                    }

                    private Map<String, BigtableTable> toTableNames(List<BigtableTable> listTables) {
                        final Map<String, BigtableTable> tables = new HashMap<>();

                        for (final BigtableTable table : listTables) {
                            tables.put(table.getName(), table);
                        }

                        return tables;
                    }

                    private Map<String, BigtableColumnFamily> toColumnFamilyNames(
                            List<BigtableColumnFamily> listColumnFamilies) {
                        final Map<String, BigtableColumnFamily> columnFamilies = new HashMap<>();

                        for (final BigtableColumnFamily family : listColumnFamilies) {
                            columnFamilies.put(family.getName(), family);
                        }

                        return columnFamilies;
                    }
                });
            }
        });
    }

    @Override
    public boolean isReady() {
        return connection.isReady();
    }

    @Override
    public Groups getGroups() {
        return groups;
    }

    @Override
    public AsyncFuture<WriteResult> write(final WriteMetric w) {
        if (w.isEmpty())
            return async.resolved(WriteResult.EMPTY);

        return connection.doto(new ManagedAction<BigtableConnection, WriteResult>() {
            @SuppressWarnings("unchecked")
            @Override
            public AsyncFuture<WriteResult> action(final BigtableConnection c) throws Exception {
                final Series series = w.getSeries();
                final List<AsyncFuture<WriteResult>> results = new ArrayList<>();

                final BigtableClient client = c.client();

                for (final MetricTypedGroup g : w.getGroups()) {
                    if (g.getType() == MetricType.POINT) {
                        for (final Point d : g.getDataAs(Point.class)) {
                            results.add(writePoint(series, client, d));
                        }
                    }
                }

                return async.collect(results, WriteResult.merger());
            }
        });
    }

    @Override
    public AsyncFuture<WriteResult> write(final Collection<WriteMetric> writes) {
        return connection.doto(new ManagedAction<BigtableConnection, WriteResult>() {
            @Override
            public AsyncFuture<WriteResult> action(final BigtableConnection c) throws Exception {
                final AtomicInteger count = new AtomicInteger(writes.size());
                final ResolvableFuture<WriteResult> future = async.future();

                for (WriteMetric w : writes) {
                    final Series series = w.getSeries();
                    final List<AsyncFuture<WriteResult>> results = new ArrayList<>();

                    final BigtableClient client = c.client();

                    for (final MetricTypedGroup g : w.getGroups()) {
                        if (g.getType() == MetricType.POINT) {
                            for (final Point d : (List<? extends Point>) g.getData()) {
                                results.add(writePoint(series, client, d));
                            }
                        }
                    }

                    async.collect(results, WriteResult.merger()).on(new FutureDone<WriteResult>() {
                        final ConcurrentLinkedQueue<WriteResult> times = new ConcurrentLinkedQueue<>();
                        final AtomicReference<Throwable> failed = new AtomicReference<>();

                        @Override
                        public void failed(Throwable cause) throws Exception {
                            check();
                            failed.compareAndSet(null, cause);
                        }

                        @Override
                        public void resolved(WriteResult result) throws Exception {
                            times.add(result);
                            check();
                        }

                        @Override
                        public void cancelled() throws Exception {
                            check();
                        }

                        private void check() {
                            if (count.decrementAndGet() == 0)
                                finish();
                        }

                        private void finish() {
                            final Throwable t = failed.get();

                            if (t != null) {
                                future.fail(t);
                                return;
                            }

                            final List<Long> times = new ArrayList<>();

                            for (final WriteResult r : this.times)
                                times.addAll(r.getTimes());

                            future.resolve(new WriteResult(times));
                        }
                    });
                }

                return future;
            }
        });
    }

    @Override
    public AsyncFuture<FetchData> fetch(MetricType type, final Series series, DateRange range,
            FetchQuotaWatcher watcher) {
        final List<PreparedQuery> prepared;

        try {
            prepared = ranges(series, range, rowKeySerializer);
        } catch (final IOException e) {
            return async.failed(e);
        }

        if (!watcher.mayReadData())
            throw new IllegalArgumentException("query violated data limit");

        return connection.doto(new ManagedAction<BigtableConnection, FetchData>() {
            @Override
            public AsyncFuture<FetchData> action(final BigtableConnection c) throws Exception {
                return fetchPoints(series, prepared, c);
            }
        });
    }

    private AsyncFuture<WriteResult> writePoint(final Series series, final BigtableClient client, final Point d)
            throws IOException {
        final long timestamp = d.getTimestamp();
        final long base = base(timestamp);
        final long offset = offset(timestamp);

        final RowKey rowKey = new RowKey(series, base);

        final ByteString rowKeyBytes = serialize(rowKey, rowKeySerializer);
        final ByteString offsetBytes = serializeOffset(offset);
        final ByteString valueBytes = serializeValue(d.getValue());

        final long start = System.nanoTime();

        final BigtableMutations mutations = client.mutations().setCell(POINTS, offsetBytes, valueBytes).build();

        final AsyncFuture<WriteResult> write = client.mutateRow(METRICS, rowKeyBytes, mutations).transform(
                new Transform<Void, WriteResult>() {
                    @Override
                    public WriteResult transform(Void result) throws Exception {
                        return WriteResult.of(System.nanoTime() - start);
                    }
                });

        return write;
    }

    private AsyncFuture<FetchData> fetchPoints(final Series series,
            final List<PreparedQuery> prepared, final BigtableConnection c) {
        final List<AsyncFuture<FetchData>> queries = new ArrayList<>();

        final BigtableClient client = c.client();

        for (final PreparedQuery p : prepared) {
            final AsyncFuture<List<BigtableLatestRow>> readRows = client.readRows(METRICS, p.keyBlob,
                    client.columnFilter(POINTS, p.startKey, p.endKey));

            final Function<BigtableCell, Point> transform = new Function<BigtableCell, Point>() {
                @Override
                public Point apply(BigtableCell cell) {
                    final long timestamp = p.base + deserializeOffset(cell.getQualifier());
                    final double value = deserializeValue(cell.getValue());
                    return new Point(timestamp, value);
                }
            };

            final long start = System.nanoTime();

            queries.add(readRows.transform(new Transform<List<BigtableLatestRow>, FetchData>() {
                @Override
                public FetchData transform(List<BigtableLatestRow> result) throws Exception {
                    final List<Iterable<Point>> points = new ArrayList<>();

                    for (final BigtableLatestRow row : result) {
                        for (final BigtableLatestColumnFamily family : row.getFamilies()) {
                            if (!family.getName().equals(POINTS)) {
                                continue;
                            }

                            points.add(Iterables.transform(family.getColumns(), transform));
                        }
                    }

                    final ImmutableList<Long> times = ImmutableList.of(System.nanoTime() - start);
                    final List<Metric> data = ImmutableList.copyOf(Iterables.mergeSorted(points,
                            Point.comparator()));
                    final List<MetricTypedGroup> groups = ImmutableList.of(new MetricTypedGroup(MetricType.POINT, data));
                    return new FetchData(series, times, groups);
                }
            }));
        }

        return async.collect(queries, FetchData.<Point> merger(series));
    }

    @Override
    public Iterable<BackendEntry> listEntries() {
        return ImmutableList.of();
    }

    @Override
    public AsyncFuture<List<BackendKey>> keys(BackendKey start, BackendKey end, int limit) {
        return async.<List<BackendKey>> resolved(ImmutableList.<BackendKey> of());
    }

    <T> ByteString serialize(T rowKey, Serializer<T> serializer) throws IOException {
        final ByteArrayOutputStream byteArray = new ByteArrayOutputStream();

        try (final SerialWriter writer = this.serializer.writeBytes()) {
            serializer.serialize(writer, rowKey);
        }

        return ByteString.copyFrom(byteArray.toByteArray());
    }

    static long base(long timestamp) {
        return timestamp - timestamp % PERIOD;
    }

    static long offset(long timestamp) {
        return timestamp % PERIOD;
    }

    List<PreparedQuery> ranges(final Series series, final DateRange range,
            final Serializer<RowKey> rowKeySerializer) throws IOException {
        final List<PreparedQuery> bases = new ArrayList<>();

        final long start = base(range.getStart());
        final long end = base(range.getEnd());

        for (long base = start; base <= end; base += PERIOD) {
            final DateRange modified = range.modify(base, base + PERIOD);

            if (modified.isEmpty())
                continue;

            final RowKey key = new RowKey(series, base);
            final ByteString keyBlob = serialize(key, rowKeySerializer);
            final ByteString startKey = serializeOffset(offset(modified.start()));
            final ByteString endKey = serializeOffset(offset(modified.end()));

            bases.add(new PreparedQuery(keyBlob, startKey, endKey, base));
        }

        return bases;
    }

    static ByteString serializeValue(double value) {
        final ByteBuffer buffer = ByteBuffer.allocate(LONG_BYTES).putLong(Double.doubleToLongBits(value));
        return ByteString.copyFrom(buffer.array());
    }

    static double deserializeValue(ByteString value) {
        return Double.longBitsToDouble(ByteBuffer.wrap(value.toByteArray()).getLong());
    }

    /**
     * Offset serialization is sensitive to byte ordering.
     *
     * We require that for two timestamps a, and b, the following invariants hold true.
     *
     * <pre>
     * offset(a) < offset(b)
     * serialized(offset(a)) < serialized(offset(b))
     * </pre>
     * 
     * Note: the serialized comparison is performed byte-by-byte, from lowest to highest address.
     *
     * @param offset
     *            Offset to serialize
     * @return A byte array, containing the serialized offset.
     */
    static ByteString serializeOffset(long offset) {
        if (offset >= PERIOD)
            throw new IllegalArgumentException("can only serialize 32-bit wide values");

        final byte bytes[] = new byte[4];
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