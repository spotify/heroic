package com.spotify.heroic.http.general;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

@Data
public class ErrorMessage {
    private final String message;

    @JsonCreator
    public static ErrorMessage create(
            @JsonProperty(value = "message", required = true) String message) {
        return new ErrorMessage(message);
    }
}