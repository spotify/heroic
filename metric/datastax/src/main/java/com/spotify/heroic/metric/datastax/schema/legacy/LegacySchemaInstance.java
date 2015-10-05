package com.spotify.heroic.metric.datastax.schema.legacy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.BackendKey;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.datastax.MetricsRowKey;
import com.spotify.heroic.metric.datastax.TypeSerializer;
import com.spotify.heroic.metric.datastax.schema.Schema.PreparedFetch;
import com.spotify.heroic.metric.datastax.schema.SchemaInstance;

import eu.toolchain.async.Transform;
import lombok.Data;

@Data
public class LegacySchemaInstance implements SchemaInstance {
    private final PreparedStatement write;
    private final PreparedStatement fetch;
    private final PreparedStatement keysPaging;
    private final PreparedStatement keysPagingLeft;
    private final PreparedStatement keysPagingLimit;
    private final PreparedStatement keysPagingLeftLimit;

    /**
     * This constant represents the maximum row width of the metrics column family. It equals the amount of numbers that
     * can be represented by Integer. Since the column name is the timestamp offset, having an integer as column offset
     * indicates that we can fit about 49 days of data into one row. We do not assume that Integers are 32 bits. This
     * makes it possible to work even if it's not 32 bits.
     */
    public static final long MAX_WIDTH = (long) Integer.MAX_VALUE - (long) Integer.MIN_VALUE + 1;

    private final TypeSerializer<MetricsRowKey> rowKey = new MetricsRowKeySerializer();

    @Override
    public TypeSerializer<MetricsRowKey> rowKey() {
        return rowKey;
    }

    @Override
    public Transform<Row, BackendKey> keyConverter() {
        return row -> {
            final MetricsRowKey key = rowKey.deserialize(row.getBytes("metric_key"));
            return new BackendKey(key.getSeries(), key.getBase());
        };
    }

    @Override
    public List<PreparedFetch> ranges(final Series series, final DateRange range) throws IOException {
        final List<PreparedFetch> bases = new ArrayList<>();

        final long start = calculateBaseTimestamp(range.getStart());
        final long end = calculateBaseTimestamp(range.getEnd());

        for (long currentBase = start; currentBase <= end; currentBase += MAX_WIDTH) {
            final DateRange modified = range.modify(currentBase, currentBase + MAX_WIDTH);

            if (modified.isEmpty()) {
                continue;
            }

            final MetricsRowKey key = new MetricsRowKey(series, currentBase);
            final ByteBuffer keyBlob = rowKey.serialize(key);
            final int startKey = calculateColumnKey(modified.start());
            final int endKey = calculateColumnKey(modified.end());
            final long base = currentBase;

            bases.add(new PreparedFetch() {
                @Override
                public BoundStatement fetch(int limit) {
                    return fetch.bind(keyBlob, startKey, endKey, limit);
                }

                @Override
                public Transform<Row, Point> converter() {
                    return row -> {
                        final long timestamp = calculateAbsoluteTimestamp(base, row.getInt(0));
                        final double value = row.getDouble(1);
                        return new Point(timestamp, value);
                    };
                }

                @Override
                public String toString() {
                    return modified.toString();
                }
            });
        }

        return bases;
    }

    @Override
    public BoundStatement keysPaging(Optional<ByteBuffer> first, int limit) {
        return first.map(f ->  keysPagingLeftLimit.bind(f, limit)).orElseGet(() -> keysPagingLimit.bind(limit));
    }

    @Override
    public WriteSession writeSession() {
        return new WriteSession() {
            final Map<Long, ByteBuffer> cache = new HashMap<>();

            @Override
            public BoundStatement writePoint(Series series, Point d) throws IOException {
                final long base = calculateBaseTimestamp(d.getTimestamp());

                ByteBuffer key = cache.get(base);

                if (key == null) {
                    key = rowKey.serialize(new MetricsRowKey(series, base));
                    cache.put(base, key);
                }

                final int offset = calculateColumnKey(d.getTimestamp());
                return write.bind(key, offset, d.getValue());
            }
        };
    }

    private long calculateBaseTimestamp(long timestamp) {
        return timestamp - timestamp % MAX_WIDTH;
    }

    private int calculateColumnKey(long timestamp) {
        // This is because column key ranges from Integer.MIN_VALUE to
        // Integer.MAX_VALUE
        final long shift = (long) Integer.MAX_VALUE + 1;

        timestamp = timestamp + shift;
        return (int) (timestamp - calculateBaseTimestamp(timestamp));
    }

    private long calculateAbsoluteTimestamp(long base, int key) {
        // This is because column key ranges from Integer.MIN_VALUE to
        // Integer.MAX_VALUE
        final long shift = (long) Integer.MAX_VALUE + 1;

        final long timestamp = base + key + shift;
        return timestamp;
    }
}