package com.spotify.heroic.http.utils;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

@Data
public class DecodeRowKeyQuery {
    private final String data;

    @JsonCreator
    public static DecodeRowKeyQuery create(@JsonProperty("data") String data) {
        return new DecodeRowKeyQuery(data);
    }
}
