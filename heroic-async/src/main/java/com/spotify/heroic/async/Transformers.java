package com.spotify.heroic.async;


public final class Transformers {
    private static final Transformer<Object, Void> TO_VOID = new Transformer<Object, Void>() {
        @Override
        public Void transform(Object result) throws Exception {
            return null;
        }
    };

    @SuppressWarnings("unchecked")
    public static <T> Transformer<T, Void> toVoid() {
        return (Transformer<T, Void>) TO_VOID;
    }

    private static final ErrorTransformer<Boolean> ERROR_TO_BOOLEAN = new ErrorTransformer<Boolean>() {
        @Override
        public Boolean transform(Exception e) throws Exception {
            return false;
        }
    };

    public static ErrorTransformer<Boolean> errorToBoolean() {
        return ERROR_TO_BOOLEAN;
    }

    private static final Transformer<Object, Boolean> TO_BOOLEAN = new Transformer<Object, Boolean>() {
        @Override
        public Boolean transform(Object result) throws Exception {
            return true;
        }
    };

    @SuppressWarnings("unchecked")
    public static <T> Transformer<T, Boolean> toBoolean() {
        return (Transformer<T, Boolean>) TO_BOOLEAN;
    }

    public static <T> Transformer<T, T> debug(final String context) {
        return new Transformer<T, T>() {
            @Override
            public T transform(T result) throws Exception {
                return result;
            }
        };
    }
}
