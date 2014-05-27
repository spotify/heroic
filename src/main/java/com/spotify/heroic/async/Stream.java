package com.spotify.heroic.async;

public class Stream<T> {
    public static interface Handle<T, R> {
        public void stream(Callback<R> callback, T result) throws Exception;

        public void close() throws Exception;
    }
}
