package com.spotify.heroic.http.model;

import java.util.Map;
import java.util.Set;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

@ToString(of = { "matchKey", "matchTags", "hasTags" })
@EqualsAndHashCode(of = { "matchKey", "matchTags", "hasTags" })
@RequiredArgsConstructor
public class TimeSeriesQuery {
    /**
     * Only include time series which match the exact key.
     */
    @Getter
    private final String matchKey;

    /**
     * Only include time series which matches the exact key/value combination.
     */
    @Getter
    private final Map<String, String> matchTags;

    /**
     * Only include time series which has the following tags.
     */
    @Getter
    private final Set<String> hasTags;

    @JsonCreator
    public static TimeSeriesQuery create(
            @JsonProperty("matchKey") String matchKey,
            @JsonProperty("matchTags") Map<String, String> matchTags,
            @JsonProperty("hasTags") Set<String> hasTags) {
        return new TimeSeriesQuery(matchKey, matchTags, hasTags);
    }
}
