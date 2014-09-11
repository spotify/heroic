package com.spotify.heroic.metrics.model;

import java.util.Map;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.exceptions.UserException;

/**
 * Indicates that a specific shard of the request failed and information on
 * which and why.
 *
 * @author udoprog
 */
@Data
public class SeriesError implements RequestError {
    private final Map<String, String> tags;
    private final String error;
    private final boolean internal;

    @JsonCreator
    public static SeriesError create(
            @JsonProperty("tags") Map<String, String> tags,
            @JsonProperty("error") String error,
            @JsonProperty("internal") Boolean internal) {
        return new SeriesError(tags, error, internal);
    }

    public static SeriesError fromException(final Map<String, String> shard,
            Exception e) {
        final String message = errorMessage(e);
        final boolean internal = !(e instanceof UserException);
        return new SeriesError(shard, message, internal);
    }

    private static String errorMessage(Throwable e) {
        final String message = e.getMessage() == null ? "<null>" : e
                .getMessage();

        if (e.getCause() == null)
            return message;

        return String.format("%s, caused by %s", message,
                errorMessage(e.getCause()));
    }
}
