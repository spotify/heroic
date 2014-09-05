package com.spotify.heroic.http.rpc3;

import java.util.List;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.metrics.model.WriteMetric;

/**
 * @author udoprog
 */
@Data
public class Rpc3WriteBody {
    private final String backendGroup;
    private final List<WriteMetric> writes;

    @JsonCreator
    public static Rpc3WriteBody create(
            @JsonProperty("backendGroup") String backendGroup,
            @JsonProperty("writes") List<WriteMetric> writes) {
        return new Rpc3WriteBody(backendGroup, writes);
    }
}
