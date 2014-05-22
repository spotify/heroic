package com.spotify.heroic.model;

import java.nio.ByteBuffer;

import com.netflix.astyanax.model.Composite;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.astyanax.serializers.LongSerializer;
import com.spotify.heroic.aggregator.AggregationGroup;
import com.spotify.heroic.aggregator.AggregationGroupSerializer;

public class CacheKeySerializer extends AbstractSerializer<CacheKey> {
    private static final TimeSerieSerializer timeSerieSerializer = TimeSerieSerializer
            .get();
    private static final AggregationGroupSerializer aggregationGroupSerializer = AggregationGroupSerializer
            .get();
    private static final LongSerializer longSerializer = LongSerializer.get();
    private static final CacheKeySerializer instance = new CacheKeySerializer();

    @Override
    public ByteBuffer toByteBuffer(CacheKey obj) {
        final Composite composite = new Composite();
        composite.addComponent(obj.getTimeSerie(), timeSerieSerializer);
        composite.addComponent(obj.getAggregationGroup(), aggregationGroupSerializer);
        return composite.serialize();
    }

    @Override
    public CacheKey fromByteBuffer(ByteBuffer byteBuffer) {
        final Composite composite = Composite.fromByteBuffer(byteBuffer);
        final TimeSerie timeSerie = composite.get(0, timeSerieSerializer);
        final AggregationGroup aggregationGroup = composite.get(1, aggregationGroupSerializer);
        final long base = composite.get(2, longSerializer);
        return new CacheKey(timeSerie, aggregationGroup, base);
    }

    public static CacheKeySerializer get() {
        return instance;
    }
}