package com.spotify.heroic.httpclient.model;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

@Data
public class ErrorMessage {
    private final String message;

    @JsonCreator
    public static ErrorMessage create(@JsonProperty("message") String message) {
        return new ErrorMessage(message);
    }
}