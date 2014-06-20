package com.spotify.heroic.async;

import com.spotify.heroic.async.Callback.Transformer;

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
}
