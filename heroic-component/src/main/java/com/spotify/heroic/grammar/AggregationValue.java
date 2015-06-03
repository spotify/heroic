package com.spotify.heroic.grammar;

import java.util.List;
import java.util.Map;

import lombok.Data;

@ValueName("aggregation")
@Data
public class AggregationValue implements Value {
    private final String name;
    private final List<Value> arguments;
    private final Map<String, Value> keywordArguments;

    @Override
    public Value sub(Value other) {
        throw new IllegalArgumentException(String.format("%s: does not support subtraction", this));
    }

    @Override
    public Value add(Value other) {
        throw new IllegalArgumentException(String.format("%s: does not support addition", this));
    }

    public String toString() {
        return "<aggregation:" + name + ":" + arguments + ":" + keywordArguments + ">";
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T cast(T to) {
        if (to instanceof AggregationValue)
            return (T) this;

        throw new ValueCastException(this, to);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T cast(Class<T> to) {
        if (to.isAssignableFrom(AggregationValue.class))
            return (T) this;

        throw new ValueTypeCastException(this, to);
    }
}