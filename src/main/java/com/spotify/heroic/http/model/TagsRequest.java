package com.spotify.heroic.http.model;

import java.util.Map;
import java.util.Set;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

@Data
public class TagsRequest {
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

    /**
     * Only include the specified tags in the result.
     */
    private final Set<String> include;

    /**
     * Exclude the specified tags in the result.
     */
    private final Set<String> exclude;

    @JsonCreator
    public static TagsRequest create(@JsonProperty("matchKey") String matchKey,
            @JsonProperty("matchTags") Map<String, String> matchTags,
            @JsonProperty("hasTags") Set<String> hasTags,
            @JsonProperty("include") Set<String> include,
            @JsonProperty("exclude") Set<String> exclude) {
        return new TagsRequest(matchKey, matchTags, hasTags, include, exclude);
    }
}
