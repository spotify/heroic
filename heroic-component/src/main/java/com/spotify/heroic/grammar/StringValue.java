package com.spotify.heroic.grammar;

import lombok.Data;

@ValueName("string")
@Data
public final class StringValue implements Value {
    private final String string;

    @Override
    public Value sub(Value other) {
        throw new IllegalArgumentException(String.format("subtraction with string is not supported", this.getClass(),
                other.getClass()));
    }

    @Override
    public Value add(Value other) {
        return new StringValue(string + other.cast(this).string);
    }

    public String toString() {
        return String.format("<string:%s>", string);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T cast(T to) {
        if (to instanceof StringValue)
            return (T) this;

        if (to instanceof Long) {
            try {
                return (T) Long.valueOf(string);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("not a valid int");
            }
        }

        throw new ValueCastException(this, to);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T cast(Class<T> to) {
        if (to.isAssignableFrom(StringValue.class))
            return (T) this;

        if (to == String.class)
            return (T) string;

        throw new ValueTypeCastException(this, to);
    }
}