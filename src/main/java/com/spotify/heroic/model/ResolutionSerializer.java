package com.spotify.heroic.model;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.astyanax.serializers.LongSerializer;

public class ResolutionSerializer extends AbstractSerializer<Resolution> {
    private static final LongSerializer serializer = LongSerializer.get();
    private static final ResolutionSerializer instance = new ResolutionSerializer();

    @Override
    public ByteBuffer toByteBuffer(Resolution obj) {
        return serializer.toByteBuffer(obj.getWidth());
    }

    @Override
    public Resolution fromByteBuffer(ByteBuffer byteBuffer) {
        final long value = serializer.fromByteBuffer(byteBuffer);
        return new Resolution(TimeUnit.MILLISECONDS, value);
    }

    public static Serializer<Resolution> get() {
        return instance;
    }
}