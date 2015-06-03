package com.spotify.heroic.filter;

import java.io.IOException;

public interface FilterJsonSerialization<T> {
    public interface Deserializer {
        /**
         * read next item as a string.
         */
        String string() throws IOException;

        /**
         * read next item as a filter.
         */
        Filter filter() throws IOException;
    }

    public interface Serializer {
        /**
         * Serialize next item as a string.
         */
        void string(String string) throws IOException;

        /**
         * Serialize next item as a filter.
         */
        void filter(Filter filter) throws IOException;
    }

    public T deserialize(Deserializer deserializer) throws IOException;

    public void serialize(Serializer serializer, T filter) throws IOException;
}
