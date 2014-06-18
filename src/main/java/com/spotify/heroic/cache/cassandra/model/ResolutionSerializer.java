package com.spotify.heroic.cache.cassandra.model;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.astyanax.serializers.LongSerializer;
import com.spotify.heroic.model.Resolution;

class ResolutionSerializer extends AbstractSerializer<Resolution> {
    private static final LongSerializer serializer = LongSerializer.get();

    @Override
    public ByteBuffer toByteBuffer(Resolution obj) {
        return serializer.toByteBuffer(obj.getWidth());
    }

    @Override
    public Resolution fromByteBuffer(ByteBuffer byteBuffer) {
        final long value = serializer.fromByteBuffer(byteBuffer);
        return new Resolution(TimeUnit.MILLISECONDS, value);
    }

    private static final ResolutionSerializer instance = new ResolutionSerializer();

    public static ResolutionSerializer get() {
        return instance;
    }
}