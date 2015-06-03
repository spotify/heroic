package com.spotify.heroic.metric.datastax.serializer;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

import com.spotify.heroic.model.Series;

public class SeriesSerializer implements CustomSerializer<Series> {
    private final CustomSerializer<String> key = new StringSerializer();
    private final CustomSerializer<Map<String, String>> tags = new MapSerializer<>(new StringSerializer(),
            new StringSerializer());

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
    public ByteBuffer serialize(Series value) {
        final TreeMap<String, String> sorted = new TreeMap<>(COMPARATOR);
        sorted.putAll(value.getTags());

        final CompositeComposer composer = new CompositeComposer();
        composer.add(value.getKey(), this.key);
        composer.add(sorted, this.tags);
        return composer.serialize();
    }

    @Override
    public Series deserialize(ByteBuffer buffer) {
        final CompositeStream reader = new CompositeStream(buffer);
        final String key = reader.next(this.key);
        final Map<String, String> tags = reader.next(this.tags);
        return new Series(key, tags);
    }
}
