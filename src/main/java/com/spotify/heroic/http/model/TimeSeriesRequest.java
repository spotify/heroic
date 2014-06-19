package com.spotify.heroic.http.model;

import java.util.Map;
import java.util.Set;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

@Data
public class TimeSeriesRequest {
    /**
     * Only include time series which match the exact key.
     */
    private final String matchKey;

    /**
     * Only include time series which matches the exact key/value combination.
     */
    private final Map<String, String> matchTags;

    /**
     * Only include time series which has the following tags.
     */
    private final Set<String> hasTags;

    @JsonCreator
    public static TimeSeriesRequest create(
            @JsonProperty("matchKey") String matchKey,
            @JsonProperty("matchTags") Map<String, String> matchTags,
            @JsonProperty("hasTags") Set<String> hasTags) {
        return new TimeSeriesRequest(matchKey, matchTags, hasTags);
    }
}
