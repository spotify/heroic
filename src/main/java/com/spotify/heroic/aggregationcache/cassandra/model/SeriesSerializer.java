package com.spotify.heroic.aggregationcache.cassandra.model;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

import com.netflix.astyanax.model.Composite;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.astyanax.serializers.MapSerializer;
import com.spotify.heroic.ext.marshal.SafeUTF8Type;
import com.spotify.heroic.ext.serializers.SafeStringSerializer;
import com.spotify.heroic.model.Series;

public class SeriesSerializer extends AbstractSerializer<Series> {
    private static final SafeStringSerializer keySerializer = SafeStringSerializer
            .get();
    private static final MapSerializer<String, String> tagsSerializer = new MapSerializer<String, String>(
            SafeUTF8Type.instance, SafeUTF8Type.instance);

    private static final SeriesSerializer instance = new SeriesSerializer();

    public static SeriesSerializer get() {
        return instance;
    }

    private static final Comparator<String> COMPARATOR = new Comparator<String>() {
        public int compare(String a, String b) {
            if (a == null || b == null) {
                if (a == null)
                    return -1;

                if (b == null)
                    return 1;

                return 0;
            }

            return a.compareTo(b);
        }
    };

    @Override
    public ByteBuffer toByteBuffer(Series obj) {
        final Composite composite = new Composite();
        final Map<String, String> tags = new TreeMap<String, String>(COMPARATOR);
        tags.putAll(obj.getTags());

        composite.addComponent(obj.getKey(), keySerializer);
        composite.addComponent(tags, tagsSerializer);

        return composite.serialize();
    }

    @Override
    public Series fromByteBuffer(ByteBuffer byteBuffer) {
        final Composite composite = Composite.fromByteBuffer(byteBuffer);

        final String key = composite.get(0, keySerializer);
        final Map<String, String> tags = composite.get(1, tagsSerializer);

        return new Series(key, tags);
    }
}