package com.spotify.heroic.metrics.heroic;

import java.nio.ByteBuffer;

import com.netflix.astyanax.model.Composite;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.astyanax.serializers.LongSerializer;
import com.spotify.heroic.cache.cassandra.model.TimeSerieSerializer;
import com.spotify.heroic.model.TimeSerie;

public class MetricsRowKeySerializer extends AbstractSerializer<MetricsRowKey> {
    public static final MetricsRowKeySerializer instance = new MetricsRowKeySerializer();

    public static MetricsRowKeySerializer get() {
        return instance;
    }

    private static final TimeSerieSerializer timeSerieSerializer = TimeSerieSerializer
            .get();
    private static final LongSerializer longSerializer = LongSerializer
            .get();

    @Override
    public ByteBuffer toByteBuffer(MetricsRowKey rowKey) {
        final Composite composite = new Composite();
        composite.addComponent(rowKey.getTimeSerie(), timeSerieSerializer);
        composite.addComponent(rowKey.getBase(), longSerializer);
        return composite.serialize();
    }

    @Override
    public MetricsRowKey fromByteBuffer(ByteBuffer byteBuffer) {
        final Composite composite = Composite.fromByteBuffer(byteBuffer);
        final TimeSerie timeSerie = composite.get(0, timeSerieSerializer);
        final Long base = composite.get(1, longSerializer);
        return new MetricsRowKey(timeSerie, base);
    }
}