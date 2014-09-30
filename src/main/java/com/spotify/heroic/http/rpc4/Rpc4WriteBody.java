package com.spotify.heroic.http.rpc4;

import java.util.Collection;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.metric.model.WriteMetric;

/**
 * @author udoprog
 */
@Data
public class Rpc4WriteBody {
    private final String backendGroup;
    private final Collection<WriteMetric> writes;

    @JsonCreator
    public static Rpc4WriteBody create(
            @JsonProperty("backendGroup") String backendGroup,
            @JsonProperty("writes") Collection<WriteMetric> writes) {
        return new Rpc4WriteBody(backendGroup, writes);
    }
}
