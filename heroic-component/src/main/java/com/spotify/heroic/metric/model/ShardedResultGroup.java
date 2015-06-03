package com.spotify.heroic.metric.model;

import java.util.List;
import java.util.Map;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(exclude = { "values" })
public final class ShardedResultGroup {
    private final Map<String, String> shard;
    private final List<TagValues> group;
    private final List<?> values;
    private final Class<?> type;

    @SuppressWarnings("unchecked")
    public <T> List<T> values(Class<T> expected) {
        if (!expected.isAssignableFrom(type))
            throw new IllegalArgumentException(String.format("does not contain values of type %s", expected));

        return (List<T>) values;
    }
}