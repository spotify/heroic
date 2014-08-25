package com.spotify.heroic.cache.cassandra.model;

import java.nio.ByteBuffer;

import com.netflix.astyanax.model.Composite;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.LongSerializer;
import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.model.Series;

public class CacheKeySerializer extends AbstractSerializer<CacheKey> {
    private static final IntegerSerializer integerSerializer = IntegerSerializer
            .get();
    private static final TimeSerieSerializer seriesSerializer = TimeSerieSerializer
            .get();
    private static final AggregationGroupSerializer aggregationGroupSerializer = AggregationGroupSerializer
            .get();
    private static final LongSerializer longSerializer = LongSerializer.get();

    @Override
    public ByteBuffer toByteBuffer(CacheKey obj) {
        final Composite composite = new Composite();
        composite.addComponent(CacheKey.VERSION, integerSerializer);
        composite.addComponent(obj.getSeries(), seriesSerializer);
        composite.addComponent(obj.getAggregationGroup(),
                aggregationGroupSerializer);
        composite.addComponent(obj.getBase(), longSerializer);
        return composite.serialize();
    }

    @Override
    public CacheKey fromByteBuffer(ByteBuffer byteBuffer) {
        final Composite composite = Composite.fromByteBuffer(byteBuffer);
        final int version = composite.get(0, integerSerializer);

        /*
         * Safety measure for upgrades. Readers should expect and handle null
         * values!
         */
        if (version != CacheKey.VERSION)
            return null;

        final Series series = composite.get(1, seriesSerializer);
        final AggregationGroup aggregationGroup = composite.get(2,
                aggregationGroupSerializer);
        final long base = composite.get(3, longSerializer);

        return new CacheKey(series, aggregationGroup, base);
    }

    private static final CacheKeySerializer instance = new CacheKeySerializer();

    public static CacheKeySerializer get() {
        return instance;
    }
}