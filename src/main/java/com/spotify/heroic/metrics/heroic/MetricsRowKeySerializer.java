package com.spotify.heroic.metrics.heroic;

import java.nio.ByteBuffer;

import com.netflix.astyanax.model.Composite;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.astyanax.serializers.LongSerializer;
import com.spotify.heroic.cache.cassandra.model.TimeSerieSerializer;
import com.spotify.heroic.model.Series;

public class MetricsRowKeySerializer extends AbstractSerializer<MetricsRowKey> {
    public static final MetricsRowKeySerializer instance = new MetricsRowKeySerializer();

    private static final TimeSerieSerializer seriesSerializer = TimeSerieSerializer
            .get();
    private static final LongSerializer longSerializer = LongSerializer.get();

    public static MetricsRowKeySerializer get() {
        return instance;
    }

    @Override
    public ByteBuffer toByteBuffer(MetricsRowKey rowKey) {
        final Composite composite = new Composite();
        composite.addComponent(rowKey.getSeries(), seriesSerializer);
        composite.addComponent(rowKey.getBase(), longSerializer);
        return composite.serialize();
    }

    @Override
    public MetricsRowKey fromByteBuffer(ByteBuffer byteBuffer) {
        final Composite composite = Composite.fromByteBuffer(byteBuffer);
        final Series series = composite.get(0, seriesSerializer);
        final Long base = composite.get(1, longSerializer);
        return new MetricsRowKey(series, base);
    }

    public static long getBaseTimestamp(long timestamp) {
        return timestamp - timestamp % MetricsRowKey.MAX_WIDTH;
    }

    public static int calculateColumnKey(long timestamp) {
        // This is because column key ranges from Integer.MIN_VALUE to
        // Integer.MAX_VALUE
        final long shift = (long) Integer.MAX_VALUE + 1;

        timestamp = timestamp + shift;
        return (int) (timestamp - MetricsRowKeySerializer
                .getBaseTimestamp(timestamp));
    }

    public static long calculateAbsoluteTimestamp(long base, int columnKey) {
        // This is because column key ranges from Integer.MIN_VALUE to
        // Integer.MAX_VALUE
        final long shift = (long) Integer.MAX_VALUE + 1;

        final long timestamp = base + columnKey + shift;
        return timestamp;
    }
}