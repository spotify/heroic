package com.spotify.heroic.model;

import java.nio.ByteBuffer;

import com.netflix.astyanax.model.Composite;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.astyanax.serializers.LongSerializer;

public class SamplingSerializer extends AbstractSerializer<Sampling> {
    private static final LongSerializer LONG_SERIALIZER = LongSerializer.get();

    private static final SamplingSerializer instance = new SamplingSerializer();

    public static SamplingSerializer get() {
        return instance;
    }

    @Override
    public ByteBuffer toByteBuffer(Sampling obj) {
        final Composite composite = new Composite();
        composite.addComponent(obj.getSize(), LONG_SERIALIZER);
        composite.addComponent(obj.getExtent(), LONG_SERIALIZER);
        return composite.serialize();
    }

    @Override
    public Sampling fromByteBuffer(ByteBuffer buffer) {
        final Composite composite = Composite.fromByteBuffer(buffer);
        final long size = composite.get(0, LONG_SERIALIZER);
        final long extent = composite.get(1, LONG_SERIALIZER);
        return new Sampling(size, extent);
    }
}