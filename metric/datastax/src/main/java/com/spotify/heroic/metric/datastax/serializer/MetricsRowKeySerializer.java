package com.spotify.heroic.metric.datastax.serializer;

import java.nio.ByteBuffer;

import com.spotify.heroic.metric.datastax.MetricsRowKey;
import com.spotify.heroic.model.Series;

public class MetricsRowKeySerializer implements CustomSerializer<MetricsRowKey> {
    private final CustomSerializer<Series> series = new SeriesSerializer();
    private final CustomSerializer<Long> longNumber = new LongSerializer();

    @Override
    public ByteBuffer serialize(MetricsRowKey value) {
        final CompositeComposer composer = new CompositeComposer();
        composer.add(value.getSeries(), this.series);
        composer.add(value.getBase(), this.longNumber);
        return composer.serialize();
    }

    @Override
    public MetricsRowKey deserialize(ByteBuffer buffer) {
        final CompositeStream reader = new CompositeStream(buffer);
        final Series series = reader.next(this.series);
        final long base = reader.next(this.longNumber);
        return new MetricsRowKey(series, base);
    }
}
