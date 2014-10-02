package com.spotify.heroic.aggregationcache.cassandra.model;

import java.nio.ByteBuffer;
import java.util.Map;

import com.netflix.astyanax.model.Composite;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.MapSerializer;
import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.aggregationcache.cassandra.filter.FilterSerializer;
import com.spotify.heroic.ext.marshal.SafeUTF8Type;
import com.spotify.heroic.filter.Filter;

public class CacheKeySerializer extends AbstractSerializer<CacheKey> {
    private static final IntegerSerializer integerSerializer = IntegerSerializer.get();
    private static final MapSerializer<String, String> groupSerializer = new MapSerializer<String, String>(
            SafeUTF8Type.instance, SafeUTF8Type.instance);
    private static final FilterSerializer filterSerializer = FilterSerializer.get();
    private static final AggregationGroupSerializer aggregationSerializer = AggregationGroupSerializer.get();
    private static final LongSerializer longSerializer = LongSerializer.get();

    @Override
    public ByteBuffer toByteBuffer(CacheKey obj) {
        final Composite composite = new Composite();
        composite.addComponent(CacheKey.VERSION, integerSerializer);

        composite.addComponent(obj.getFilter(), filterSerializer);
        composite.addComponent(obj.getGroup(), groupSerializer);
        composite.addComponent(obj.getAggregation(), aggregationSerializer);
        composite.addComponent(obj.getBase(), longSerializer);

        return composite.serialize();
    }

    @Override
    public CacheKey fromByteBuffer(ByteBuffer byteBuffer) {
        final Composite composite = Composite.fromByteBuffer(byteBuffer);
        final int version = composite.get(0, integerSerializer);

        /* Safety measure for upgrades. Readers should expect and handle null values! */
        if (version != CacheKey.VERSION)
            return null;

        final Filter filter = composite.get(1, filterSerializer);
        final Map<String, String> group = composite.get(2, groupSerializer);
        final AggregationGroup aggregation = composite.get(3, aggregationSerializer);
        final Long base = composite.get(4, longSerializer);

        return new CacheKey(version, filter, group, aggregation, base);
    }

    private static final CacheKeySerializer instance = new CacheKeySerializer();

    public static CacheKeySerializer get() {
        return instance;
    }
}