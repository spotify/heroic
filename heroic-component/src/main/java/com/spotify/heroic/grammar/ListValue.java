package com.spotify.heroic.grammar;

import java.util.ArrayList;
import java.util.List;

import lombok.Data;

@ValueName("list")
@Data
public final class ListValue implements Value {
    private final List<Value> list;

    @Override
    public Value sub(Value other) {
        throw new IllegalArgumentException("list does not support subtraction");
    }

    @Override
    public Value add(Value other) {
        final ListValue o = other.cast(this);
        final ArrayList<Value> list = new ArrayList<Value>();
        list.addAll(this.list);
        list.addAll(o.list);
        return new ListValue(list);
    }

    public String toString() {
        return "<list:" + list + ">";
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T cast(T to) {
        if (to instanceof ListValue)
            return (T) this;

        throw new ValueCastException(this, to);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T cast(Class<T> to) {
        if (to.isAssignableFrom(ListValue.class))
            return (T) this;

        throw new ValueTypeCastException(this, to);
    }
}