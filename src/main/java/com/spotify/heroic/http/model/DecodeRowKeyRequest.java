package com.spotify.heroic.http.model;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

@Data
public class DecodeRowKeyRequest {
    private final String data;

    @JsonCreator
    public static DecodeRowKeyRequest create(@JsonProperty("data") String data) {
        return new DecodeRowKeyRequest(data);
    }
}
