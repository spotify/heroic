package com.spotify.heroic.model;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.cassandra.db.marshal.UTF8Type;

import com.netflix.astyanax.model.Composite;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.astyanax.serializers.MapSerializer;
import com.netflix.astyanax.serializers.StringSerializer;

public class TimeSerieSerializer extends AbstractSerializer<TimeSerie> {
    private static final StringSerializer keySerializer = StringSerializer
            .get();
    private static final MapSerializer<String, String> tagsSerializer = new MapSerializer<String, String>(
            UTF8Type.instance, UTF8Type.instance);

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
        final Map<String, String> tags = composite.get(0, tagsSerializer);

        return new TimeSerie(key, tags);
    }
}