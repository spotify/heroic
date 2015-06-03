package com.spotify.heroic.metric.model;

import java.util.List;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

@Data
public final class ResultGroup {
    private final List<TagValues> tags;
    private final List<?> values;
    private final Class<?> type;

    @SuppressWarnings("unchecked")
    public <T> List<T> valuesFor(Class<T> expected) {
        if (!expected.isAssignableFrom(type))
            throw new RuntimeException(String.format("incompatible payload type between %s (expected) and %s (actual)",
                    expected.getCanonicalName(), type.getCanonicalName()));

        return (List<T>) values;
    }

    @JsonCreator
    public ResultGroup(@JsonProperty("tags") List<TagValues> tags, @JsonProperty("values") List<?> values,
            @JsonProperty("type") Class<?> type) {
        this.tags = tags;
        this.values = values;
        this.type = type;
    }
}