package com.spotify.heroic.metadata.model;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

@Data
final public class AddSeries {
    private final String id;

    @JsonCreator
    public static AddSeries create(@JsonProperty("id") String id) {
        return new AddSeries(id);
    }
}