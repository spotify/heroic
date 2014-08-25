package com.spotify.heroic.http.utils;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

@Data
public class UtilsDecodeRowKeyQuery {
    private final String data;

    @JsonCreator
    public static UtilsDecodeRowKeyQuery create(@JsonProperty("data") String data) {
        return new UtilsDecodeRowKeyQuery(data);
    }
}
