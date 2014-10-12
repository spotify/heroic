package com.spotify.heroic.async;


public final class Transformers {
    private static final Transform<Object, Void> TO_VOID = new Transform<Object, Void>() {
        @Override
        public Void transform(Object result) throws Exception {
            return null;
        }
    };

    @SuppressWarnings("unchecked")
    public static <T> Transform<T, Void> toVoid() {
        return (Transform<T, Void>) TO_VOID;
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

    private static final Transform<Object, Boolean> TO_BOOLEAN = new Transform<Object, Boolean>() {
        @Override
        public Boolean transform(Object result) throws Exception {
            return true;
        }
    };

    @SuppressWarnings("unchecked")
    public static <T> Transform<T, Boolean> toBoolean() {
        return (Transform<T, Boolean>) TO_BOOLEAN;
    }

    public static <T> Transform<T, T> debug(final String context) {
        return new Transform<T, T>() {
            @Override
            public T transform(T result) throws Exception {
                return result;
            }
        };
    }
}
