package com.spotify.heroic.http.rpc5;

import java.util.Collection;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.metrics.model.WriteMetric;

/**
 * @author udoprog
 */
@Data
public class Rpc5WriteBody {
    private final String backendGroup;
    private final Collection<WriteMetric> writes;

    @JsonCreator
    public static Rpc5WriteBody create(
            @JsonProperty("backendGroup") String backendGroup,
            @JsonProperty("writes") Collection<WriteMetric> writes) {
        return new Rpc5WriteBody(backendGroup, writes);
    }
}
