package com.spotify.heroic.grammar;

import java.util.concurrent.TimeUnit;

import lombok.Data;

/**
 * int's are represented internally as longs.
 *
 * @author udoprog
 */
@ValueName("int")
@Data
public final class IntValue implements Value {
    private final Long value;

    @Override
    public Value sub(Value other) {
        return new IntValue(value - other.cast(this).value);
    }

    @Override
    public Value add(Value other) {
        return new IntValue(value + other.cast(this).value);
    }

    public String toString() {
        return String.format("<int:%d>", value);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T cast(T to) {
        if (to instanceof IntValue)
            return (T) this;

        if (to instanceof DiffValue) {
            final DiffValue o = (DiffValue) to;
            return (T) new DiffValue(o.getUnit(), o.getUnit().convert(value, TimeUnit.MILLISECONDS));
        }

        throw new ValueCastException(this, to);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T cast(Class<T> to) {
        if (to.isAssignableFrom(IntValue.class))
            return (T) this;

        if (to.isAssignableFrom(Long.class))
            return (T) value;

        throw new ValueTypeCastException(this, to);
    }
}