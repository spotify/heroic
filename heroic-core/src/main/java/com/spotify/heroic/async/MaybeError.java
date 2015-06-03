package com.spotify.heroic.async;

import lombok.Data;
import eu.toolchain.async.Transform;

/**
 * A type to encapsulate a value with two possibilities, A value, or an error.
 *
 * @param <T> The type of the value.
 */
@Data
public final class MaybeError<T> {
    private final boolean just;
    private final Object data;

    private MaybeError(boolean just, Object data) {
        this.just = just;
        this.data = data;
    }

    @SuppressWarnings("unchecked")
    public T getJust() {
        if (!just)
            throw new IllegalStateException("not a value");

        return (T) data;
    }

    public Throwable getError() {
        if (just)
            throw new IllegalStateException("not an error");

        return (Throwable) data;
    }

    public boolean isJust() {
        return just;
    }

    public boolean isError() {
        return !just;
    }

    /**
     * Create a new Maybe which holds a value.
     *
     * @param a
     * @return
     */
    public static <A> MaybeError<A> just(A a) {
        return new MaybeError<A>(true, a);
    }

    /**
     * Create a new Maybe which holds an error.
     *
     * @param error
     * @return
     */
    public static <A> MaybeError<A> error(Throwable error) {
        return new MaybeError<A>(false, error);
    }

    private static final Transform<? extends Object, ? extends MaybeError<? extends Object>> toJust = new Transform<Object, MaybeError<Object>>() {
        @Override
        public MaybeError<Object> transform(Object result) throws Exception {
            return MaybeError.just(result);
        }
    };

    @SuppressWarnings("unchecked")
    public static <A> Transform<A, MaybeError<A>> transformJust() {
        return (Transform<A, MaybeError<A>>) toJust;
    }
}
