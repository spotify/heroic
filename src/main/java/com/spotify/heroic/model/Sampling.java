package com.spotify.heroic.model;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

@Data
public class Sampling {
    private final long size;
    private final long extent;

    @JsonCreator
    public static Sampling create(@JsonProperty(value = "size", required = true) Long size,
            @JsonProperty(value = "extent", required = true) Long extent) {
        return new Sampling(size, extent);
    }
}
