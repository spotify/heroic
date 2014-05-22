package com.spotify.heroic.model;

import java.nio.ByteBuffer;
import java.util.Map;

import com.netflix.astyanax.model.Composite;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.astyanax.serializers.MapSerializer;
import com.spotify.heroic.marshal.SafeUTF8Type;
import com.spotify.heroic.serializers.SafeStringSerializer;

public class TimeSerieSerializer extends AbstractSerializer<TimeSerie> {
    private static final SafeStringSerializer keySerializer = SafeStringSerializer
            .get();
    private static final MapSerializer<String, String> tagsSerializer = new MapSerializer<String, String>(
            SafeUTF8Type.instance, SafeUTF8Type.instance);

    private static final TimeSerieSerializer instance = new TimeSerieSerializer();

    public static TimeSerieSerializer get() {
        return instance;
    }

    @Override
    public ByteBuffer toByteBuffer(TimeSerie obj) {
        final Composite composite = new Composite();

        composite.addComponent(obj.getKey(), keySerializer);
        composite.addComponent(obj.getTags(), tagsSerializer);

        return composite.serialize();
    }

    @Override
    public TimeSerie fromByteBuffer(ByteBuffer byteBuffer) {
        final Composite composite = Composite.fromByteBuffer(byteBuffer);

        final String key = composite.get(0, keySerializer);
        final Map<String, String> tags = composite.get(1, tagsSerializer);

        return new TimeSerie(key, tags);
    }
}