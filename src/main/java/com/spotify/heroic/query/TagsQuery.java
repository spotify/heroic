package com.spotify.heroic.query;

import java.util.Map;
import java.util.Set;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@ToString(of = { "matchKey", "matchTags", "hasTags", "include", "exclude" })
@EqualsAndHashCode(of = { "matchKey", "matchTags", "hasTags", "include",
        "exclude" })
public class TagsQuery {
    /**
     * Only include time series which match the exact key.
     */
    @Getter
    @Setter
    private String matchKey;

    /**
     * Only include time series which matches the exact key/value combination.
     */
    @Getter
    @Setter
    private Map<String, String> matchTags;

    /**
     * Only include time series which has the following tags.
     */
    @Getter
    @Setter
    private Set<String> hasTags;

    /**
     * Only include the specified tags in the result.
     */
    @Getter
    @Setter
    private Set<String> include;

    /**
     * Exclude the specified tags in the result.
     */
    @Getter
    @Setter
    private Set<String> exclude;
}
