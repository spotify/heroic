package com.spotify.heroic.model;

import java.nio.ByteBuffer;

import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.model.Composite;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.spotify.heroic.aggregator.Aggregation;
import com.spotify.heroic.aggregator.AggregationSerializer;

public class CacheKeySerializer extends AbstractSerializer<CacheKey> {
    private static final TimeSerieSerializer timeSerieSerializer = TimeSerieSerializer
            .get();
    private static final AggregationSerializer aggregationSerializer = AggregationSerializer
            .get();
    private static final CacheKeySerializer instance = new CacheKeySerializer();

    @Override
    public ByteBuffer toByteBuffer(CacheKey obj) {
        final Composite composite = new Composite();
        composite.addComponent(obj.getTimeSerie(), timeSerieSerializer);
        composite.addComponent(obj.getAggregation(), aggregationSerializer);
        return composite.serialize();
    }

    @Override
    public CacheKey fromByteBuffer(ByteBuffer byteBuffer) {
        final Composite composite = Composite.fromByteBuffer(byteBuffer);
        final TimeSerie timeSerie = composite.get(0, timeSerieSerializer);
        final Aggregation aggregation = composite.get(1, aggregationSerializer);
        return new CacheKey(timeSerie, aggregation);
    }

    public static Serializer<CacheKey> get() {
        return instance;
    }
}