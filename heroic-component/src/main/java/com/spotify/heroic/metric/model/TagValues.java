package com.spotify.heroic.metric.model;

import java.util.List;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

@Data
public class TagValues {
    private final String key;
    private final List<String> values;

    @JsonCreator
    public TagValues(@JsonProperty("key") String key, @JsonProperty("values") List<String> values) {
        this.key = key;
        this.values = values;
    }
}