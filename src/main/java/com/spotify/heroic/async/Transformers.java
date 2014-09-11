package com.spotify.heroic.async;

import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.async.Callback.Transformer;

@Slf4j
public final class Transformers {
    private static final Callback.Transformer<Object, Void> TO_VOID = new Callback.Transformer<Object, Void>() {
        @Override
        public Void transform(Object result) throws Exception {
            return null;
        }
    };

    @SuppressWarnings("unchecked")
    public static <T> Callback.Transformer<T, Void> toVoid() {
        return (Transformer<T, Void>) TO_VOID;
    }

    public static <T> Callback.Transformer<T, T> debug(final String context) {
        return new Callback.Transformer<T, T>() {
            @Override
            public T transform(T result) throws Exception {
                log.info("{}: {}", context, result);
                return result;
            }
        };
    }
}
