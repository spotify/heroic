package com.spotify.heroic.async;

public class Stream<T> {
    public static interface Handle<T> {
        public void stream(T result) throws Exception;

        public void close() throws Exception;
    }
}
