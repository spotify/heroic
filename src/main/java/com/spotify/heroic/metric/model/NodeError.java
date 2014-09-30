package com.spotify.heroic.metric.model;

import java.net.URI;
import java.util.Map;
import java.util.UUID;

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
public class NodeError implements RequestError {
    private final UUID nodeId;
    private final URI nodeUri;
    private final Map<String, String> tags;
    private final String error;
    private final boolean internal;

    @JsonCreator
    public static NodeError create(@JsonProperty("nodeId") UUID nodeId,
            @JsonProperty("nodeUri") URI nodeUri,
            @JsonProperty("tags") Map<String, String> tags,
            @JsonProperty("error") String error,
            @JsonProperty("internal") Boolean internal) {
        return new NodeError(nodeId, nodeUri, tags, error, internal);
    }

    public static NodeError fromException(final UUID nodeId,
            final URI nodeUri, final Map<String, String> shard, Exception e) {
        final String message = errorMessage(e);
        final boolean internal = !(e instanceof UserException);
        return new NodeError(nodeId, nodeUri, shard, message, internal);
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
