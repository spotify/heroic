package com.spotify.heroic.filter.cassandra;

import java.nio.ByteBuffer;

import com.netflix.astyanax.model.Composite;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.model.CacheKey;

/**
 * Serializes aggregation configurations.
 *
 * Each aggregation configuration is packed into a Composite which has the type of the aggregation as a prefixed short.
 *
 * @author udoprog
 */
public class FilterSerializer extends AbstractSerializer<Filter> {
    private static final FilterSerializer instance = new FilterSerializer();

    public static FilterSerializer get() {
        return instance;
    }

    private static final IntegerSerializer integerSerializer = IntegerSerializer.get();

    private static final ByteBuffer ZERO_BUFFER = ByteBuffer.allocate(0);

    @Override
    public ByteBuffer toByteBuffer(final Filter filter) {
        if (filter == null)
            return ZERO_BUFFER;

        final Filter optimized = filter.optimize();

        if (optimized == null)
            return ZERO_BUFFER;

        final Composite c = new Composite();

        final int typeId = CassandraCommon.getTypeId(optimized.getClass());

        final FilterSerialization<Filter> serializer = CassandraCommon.getSerializer(typeId);

        c.addComponent(CacheKey.VERSION, integerSerializer);
        c.addComponent(typeId, integerSerializer);

        serializer.serialize(c, optimized);

        return c.serialize();
    }

    @Override
    public Filter fromByteBuffer(ByteBuffer byteBuffer) {
        if (byteBuffer.remaining() == 0)
            return null;

        final Composite c = Composite.fromByteBuffer(byteBuffer);

        final int version = c.get(0, integerSerializer);

        if (version != CacheKey.VERSION)
            return null;

        final int typeId = c.get(1, integerSerializer);

        final FilterSerialization<Filter> serializer = CassandraCommon.getSerializer(typeId);

        return serializer.deserialize(c);
    }
}